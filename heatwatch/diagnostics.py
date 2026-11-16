from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta

from .contracts import DomainError, utc_time
from .storage import Store


class DiagnosticService:
    def __init__(self, store: Store):
        self.store = store

    def batch_quality(self, from_at: str, to_at: str) -> dict:
        start = utc_time(from_at, "from_at")
        end = utc_time(to_at, "to_at")
        if start > end:
            raise DomainError("invalid_window", "from_at exceeds to_at")
        rows = self.store.connection.execute("""SELECT batch_id,station,sensor,accepted,reasons,
            observed_at FROM observations WHERE observed_at BETWEEN ? AND ? ORDER BY id""",
            (start, end)).fetchall()
        reasons = Counter()
        stations = defaultdict(lambda: {"accepted": 0, "rejected": 0, "sensors": set()})
        batches = defaultdict(lambda: {"accepted": 0, "rejected": 0})
        for row in rows:
            decision = "accepted" if row["accepted"] else "rejected"
            stations[row["station"]][decision] += 1
            stations[row["station"]]["sensors"].add(row["sensor"])
            batches[row["batch_id"]][decision] += 1
            reasons.update(__import__("json").loads(row["reasons"]))
        return {"from_at": start, "to_at": end,
                "stations": {code: {"accepted": item["accepted"],
                                     "rejected": item["rejected"],
                                     "sensors": sorted(item["sensors"])}
                             for code, item in sorted(stations.items())},
                "batches": dict(sorted(batches.items())),
                "rejection_reasons": dict(sorted(reasons.items()))}

    def gaps(self, sensor: str, from_sequence: int = 0) -> dict:
        if type(from_sequence) is not int or from_sequence < 0:
            raise DomainError("invalid_sequence", "from_sequence must be nonnegative")
        rows = self.store.connection.execute("""SELECT sequence,accepted,observed_at
            FROM observations WHERE sensor=? AND sequence>=? ORDER BY sequence,id""",
            (sensor, from_sequence)).fetchall()
        present = {row["sequence"] for row in rows}
        accepted = {row["sequence"] for row in rows if row["accepted"]}
        missing = []
        if rows:
            upper = max(present)
            for sequence in range(from_sequence, upper + 1):
                if sequence not in present:
                    missing.append(sequence)
        return {"sensor": sensor, "from_sequence": from_sequence,
                "present": sorted(present), "accepted": sorted(accepted),
                "missing": missing, "rejected": sorted(present - accepted)}

    def station_continuity(self, station: str, from_at: str, to_at: str,
                           expected_minutes: int) -> dict:
        start = utc_time(from_at, "from_at")
        end = utc_time(to_at, "to_at")
        if start > end:
            raise DomainError("invalid_window", "from_at exceeds to_at")
        if type(expected_minutes) is not int or not 1 <= expected_minutes <= 1440:
            raise DomainError("invalid_interval", "expected_minutes must be between 1 and 1440")
        if station not in self.store.network().stations:
            raise DomainError("unknown_station", "station does not exist")
        timestamps = [datetime.fromisoformat(row[0]) for row in self.store.connection.execute(
            """SELECT DISTINCT observed_at FROM observations WHERE station=? AND accepted=1
            AND observed_at BETWEEN ? AND ? ORDER BY observed_at""", (station, start, end))]
        interval = timedelta(minutes=expected_minutes)
        boundaries = [datetime.fromisoformat(start), *timestamps, datetime.fromisoformat(end)]
        gaps = []
        for left, right in zip(boundaries, boundaries[1:]):
            delta = right - left
            if delta > interval:
                gaps.append({"after": left.isoformat(), "before": right.isoformat(),
                             "minutes": delta.total_seconds() / 60})
        return {"station": station, "from_at": start, "to_at": end,
                "expected_minutes": expected_minutes,
                "accepted_samples": len(timestamps), "gaps": gaps,
                "continuous": not gaps}

    def incident_evidence(self, incident_id: int) -> dict:
        incident = self.store.incident(incident_id)
        events = self.store.events(incident_id)
        rows = self.store.connection.execute("""SELECT id,observed_at,source_id,sequence,
            supply_c,return_c,flow_m3h,pressure_kpa FROM observations WHERE id BETWEEN ? AND ?
            AND station=? AND accepted=1 ORDER BY id""",
            (incident["first_observation"], incident["last_observation"],
             incident["station"])).fetchall()
        return {"incident": incident, "events": events,
                "observations": [dict(row) for row in rows],
                "evidence_complete": bool(rows) and rows[0]["id"] == incident["first_observation"],
                "source_ids": [row["source_id"] for row in rows]}

    def open_risk_by_zone(self) -> dict:
        network = self.store.network()
        zones = defaultdict(lambda: {"incidents": 0, "households": set(),
                                     "stations": set(), "critical": 0})
        for incident in self.store.incidents("open"):
            zone = network.stations[incident["station"]].zone
            row = zones[zone]
            row["incidents"] += 1
            row["critical"] += incident["severity"] == "critical"
            for station in incident["affected_stations"]:
                row["stations"].add(station)
                row["households"].add(station)
        return {zone: {"incidents": value["incidents"],
                       "critical": value["critical"],
                       "stations": sorted(value["stations"]),
                       "households": sum(network.stations[s].households
                                         for s in value["households"])}
                for zone, value in sorted(zones.items())}
