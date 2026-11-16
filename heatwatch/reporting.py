from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime
from statistics import mean

from .contracts import DomainError, utc_time
from .storage import Store


class ReportingService:
    def __init__(self, store: Store):
        self.store = store

    def window(self, from_at: str, to_at: str) -> dict:
        from_at = utc_time(from_at, "from_at")
        to_at = utc_time(to_at, "to_at")
        if from_at > to_at:
            raise DomainError("invalid_window", "from_at must not exceed to_at")
        observations = self.store.observations(from_at=from_at, to_at=to_at)
        grouped = defaultdict(list)
        rejected = defaultdict(int)
        for row in observations:
            if row["accepted"]:
                grouped[row["station"]].append(row)
            else:
                rejected[row["station"]] += 1
        stations = []
        for station in sorted(set(grouped) | set(rejected)):
            records = grouped[station]
            def average(field: str):
                return round(mean(float(row[field]) for row in records), 3) if records else None
            stations.append({"station": station, "accepted": len(records),
                             "rejected": rejected[station],
                             "average_supply_c": average("supply_c"),
                             "average_return_c": average("return_c"),
                             "average_flow_m3h": average("flow_m3h"),
                             "average_pressure_kpa": average("pressure_kpa"),
                             "gas_m3": round(sum(float(row["gas_m3h"]) for row in records), 3),
                             "first_at": records[0]["observed_at"] if records else None,
                             "last_at": records[-1]["observed_at"] if records else None})
        incidents = [item for item in self.store.incidents()
                     if from_at <= item["opened_at"] <= to_at]
        return {"from_at": from_at, "to_at": to_at,
                "accepted": sum(s["accepted"] for s in stations),
                "rejected": sum(s["rejected"] for s in stations),
                "stations": stations,
                "incidents": [{"id": i["id"], "station": i["station"],
                               "status": i["status"], "rule": i["rule"],
                               "households": i["affected_households"]} for i in incidents]}

    def reconcile(self) -> dict:
        batch_row = self.store.connection.execute("""SELECT COALESCE(SUM(accepted),0),
            COALESCE(SUM(rejected),0) FROM batches""").fetchone()
        accepted = self.store.count_observations(True)
        rejected = self.store.count_observations(False)
        orphaned = self.store.connection.execute("""SELECT COUNT(*) FROM observations o
            LEFT JOIN batches b ON b.batch_id=o.batch_id WHERE b.batch_id IS NULL""").fetchone()[0]
        open_incidents = self.store.connection.execute(
            "SELECT COUNT(*) FROM incidents WHERE status='open'").fetchone()[0]
        active_states = self.store.connection.execute(
            "SELECT COUNT(*) FROM rule_state WHERE active_incident IS NOT NULL").fetchone()[0]
        checks = {"batch_accepted": batch_row[0] == accepted,
                  "batch_rejected": batch_row[1] == rejected,
                  "no_orphan_observations": orphaned == 0,
                  "active_state_matches_incidents": active_states == open_incidents}
        return {"ok": all(checks.values()), "checks": checks,
                "counts": {"accepted": accepted, "rejected": rejected,
                           "open_incidents": open_incidents, "orphaned": orphaned}}

    def export(self, from_at: str, to_at: str, created_at: str) -> dict:
        report = self.window(from_at, to_at)
        if not self.reconcile()["ok"]:
            raise DomainError("reconciliation_failed", "cannot export an inconsistent ledger")
        payload = json.dumps(report, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(payload.encode()).hexdigest()
        created_at = utc_time(created_at, "created_at")
        with self.store.transaction():
            cursor = self.store.connection.execute("""INSERT INTO exports(
                created_at,from_at,to_at,payload,digest) VALUES(?,?,?,?,?)""",
                (created_at, report["from_at"], report["to_at"], payload, digest))
        return {"id": cursor.lastrowid, "digest": digest, "report": report}

    def saved_export(self, export_id: int) -> dict:
        row = self.store.connection.execute("SELECT * FROM exports WHERE id=?", (export_id,)).fetchone()
        if row is None:
            raise DomainError("unknown_export", "export does not exist")
        if hashlib.sha256(row["payload"].encode()).hexdigest() != row["digest"]:
            raise DomainError("export_corrupt", "export digest mismatch")
        return {"id": row["id"], "created_at": row["created_at"],
                "digest": row["digest"], "report": json.loads(row["payload"])}
