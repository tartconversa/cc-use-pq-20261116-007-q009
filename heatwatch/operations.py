from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from .contracts import DomainError, required_text, utc_time
from .storage import Store


class OperationsService:
    def __init__(self, store: Store):
        self.store = store

    def freshness(self, now: str, allowed_minutes: int = 30) -> dict:
        now = utc_time(now, "now")
        if type(allowed_minutes) is not int or not 0 <= allowed_minutes <= 10080:
            raise DomainError("invalid_freshness", "allowed_minutes must be a bounded integer")
        cutoff = datetime.fromisoformat(now) - timedelta(minutes=allowed_minutes)
        network = self.store.network()
        rows = self.store.connection.execute("""SELECT station,sensor,MAX(observed_at) AS latest
            FROM observations WHERE accepted=1 GROUP BY station,sensor ORDER BY station,sensor""").fetchall()
        by_station = defaultdict(list)
        for row in rows:
            item = {"sensor": row["sensor"], "latest_at": row["latest"],
                    "fresh": datetime.fromisoformat(row["latest"]) >= cutoff}
            by_station[row["station"]].append(item)
        stations = []
        for code, station in network.stations.items():
            sensors = by_station.get(code, [])
            stations.append({"station": code, "zone": station.zone,
                             "sensors": sensors, "fresh": bool(sensors) and any(s["fresh"] for s in sensors),
                             "households": station.households})
        stale = [s for s in stations if not s["fresh"]]
        return {"as_of": now, "allowed_minutes": allowed_minutes,
                "stations": stations, "stale_stations": [s["station"] for s in stale],
                "at_risk_households": sum(s["households"] for s in stale)}

    def sensor_history(self, sensor: str) -> dict:
        sensor = required_text(sensor, "sensor")
        rows = [dict(row) for row in self.store.connection.execute("""SELECT id,station,sequence,
            observed_at,received_at,accepted,reasons,source_id FROM observations
            WHERE sensor=? ORDER BY sequence,id""", (sensor,))]
        for row in rows:
            row["reasons"] = __import__("json").loads(row["reasons"])
            row["accepted"] = bool(row["accepted"])
        accepted = [r for r in rows if r["accepted"]]
        gaps = []
        for before, after in zip(accepted, accepted[1:]):
            if after["sequence"] > before["sequence"] + 1:
                gaps.append({"from": before["sequence"] + 1, "to": after["sequence"] - 1})
        return {"sensor": sensor, "observations": rows, "sequence_gaps": gaps,
                "accepted": len(accepted), "rejected": len(rows) - len(accepted)}

    def incident_workload(self, from_at: str, to_at: str) -> dict:
        from_at = utc_time(from_at, "from_at")
        to_at = utc_time(to_at, "to_at")
        if from_at > to_at:
            raise DomainError("invalid_window", "from_at exceeds to_at")
        incidents = [i for i in self.store.incidents() if from_at <= i["opened_at"] <= to_at]
        teams = defaultdict(lambda: {"assigned": 0, "resolved": 0, "cancelled": 0})
        for incident in incidents:
            for dispatch in self.store.connection.execute(
                    "SELECT team,status FROM dispatches WHERE incident_id=?", (incident["id"],)):
                teams[dispatch["team"]]["assigned"] += 1
                if dispatch["status"] in {"resolved", "cancelled"}:
                    teams[dispatch["team"]][dispatch["status"]] += 1
        return {"from_at": from_at, "to_at": to_at,
                "incident_count": len(incidents),
                "open_count": sum(i["status"] == "open" for i in incidents),
                "critical_count": sum(i["severity"] == "critical" for i in incidents),
                "affected_households": sum(i["affected_households"] for i in incidents),
                "teams": {name: dict(value) for name, value in sorted(teams.items())}}

    def audit(self) -> dict:
        issues = []
        state_rows = self.store.connection.execute("SELECT * FROM rule_state").fetchall()
        for state in state_rows:
            if state["active_incident"] is None:
                continue
            try:
                incident = self.store.incident(state["active_incident"])
            except DomainError:
                issues.append({"code": "missing_incident", "station": state["station"],
                               "rule": state["rule"]})
                continue
            if incident["status"] != "open" or incident["station"] != state["station"] or incident["rule"] != state["rule"]:
                issues.append({"code": "state_incident_mismatch", "station": state["station"],
                               "rule": state["rule"]})
        for incident in self.store.incidents("open"):
            state = self.store.rule_state(incident["station"], incident["rule"])
            if state["active_incident"] != incident["id"]:
                issues.append({"code": "orphan_open_incident", "incident_id": incident["id"]})
        for row in self.store.connection.execute("SELECT * FROM dispatches"):
            incident = self.store.incident(row["incident_id"])
            if incident["status"] == "closed" and row["status"] in {"assigned", "en_route", "on_site"}:
                issues.append({"code": "dispatch_after_close", "dispatch_id": row["id"]})
        return {"ok": not issues, "issues": issues,
                "states_checked": len(state_rows),
                "incidents_checked": len(self.store.incidents())}
