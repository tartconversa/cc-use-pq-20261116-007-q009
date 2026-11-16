from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from .capacity import CapacityPlanner
from .contracts import DomainError, number, required_text, utc_time
from .storage import Store


class ResponsePlanner:
    def __init__(self, store: Store):
        self.store = store

    def incident_priority(self, incident_id: int, now: str) -> dict:
        incident = self.store.incident(incident_id)
        now = datetime.fromisoformat(utc_time(now, "now"))
        opened = datetime.fromisoformat(incident["opened_at"])
        age_minutes = max(0, (now - opened).total_seconds() / 60)
        severity_weight = 3 if incident["severity"] == "critical" else 1
        household_weight = min(10, incident["affected_households"] / 1000)
        age_weight = min(5, age_minutes / 60)
        dispatches = [dict(row) for row in self.store.connection.execute(
            "SELECT * FROM dispatches WHERE incident_id=?", (incident_id,))]
        active = any(item["status"] in {"en_route", "on_site"} for item in dispatches)
        score = severity_weight * 10 + household_weight * 3 + age_weight * 2
        if active:
            score *= 0.8
        return {"incident_id": incident_id, "score": round(score, 3),
                "severity": incident["severity"],
                "affected_households": incident["affected_households"],
                "age_minutes": round(age_minutes, 1), "team_active": active,
                "status": incident["status"]}

    def queue(self, now: str) -> dict:
        priorities = [self.incident_priority(item["id"], now)
                      for item in self.store.incidents("open")]
        priorities.sort(key=lambda item: (-item["score"], item["incident_id"]))
        return {"as_of": utc_time(now, "now"), "incidents": priorities,
                "unassigned": [item["incident_id"] for item in priorities
                               if not self.store.connection.execute(
                                   "SELECT 1 FROM dispatches WHERE incident_id=? LIMIT 1",
                                   (item["incident_id"],)).fetchone()]}

    def isolate_pipe(self, pipe_code: str, demands: list[dict],
                     reserve_fraction: float = 0) -> dict:
        pipe_code = required_text(pipe_code, "pipe_code")
        network = self.store.network()
        if pipe_code not in network.pipes:
            raise DomainError("unknown_pipe", "pipe does not exist")
        before = CapacityPlanner(network).plan(demands, reserve_fraction=reserve_fraction)
        from .topology import Network, Pipe
        replacement = []
        for pipe in network.pipes.values():
            if pipe.code == pipe_code:
                replacement.append(Pipe(pipe.code, pipe.upstream, pipe.downstream,
                                        pipe.max_flow_m3h, "closed"))
            else:
                replacement.append(pipe)
        isolated = Network(list(network.stations.values()), replacement)
        after = CapacityPlanner(isolated).plan(demands, reserve_fraction=reserve_fraction)
        before_unmet = {item["station"]: item["unmet_mw"] for item in before["unmet"]}
        after_unmet = {item["station"]: item["unmet_mw"] for item in after["unmet"]}
        return {"pipe": pipe_code, "before": before, "after": after,
                "new_shortfall_mw": round(sum(after_unmet.values()) - sum(before_unmet.values()), 6),
                "newly_unmet": sorted(set(after_unmet) - set(before_unmet)),
                "downstream_stations": isolated.affected(network.pipes[pipe_code].downstream)}

    def station_source_mix(self, station: str) -> dict:
        network = self.store.network()
        sources = network.reachable_sources(station)
        by_type = defaultdict(lambda: {"capacity_mw": 0.0, "stations": []})
        for code in sources:
            item = network.stations[code]
            by_type[item.source]["capacity_mw"] += item.capacity_mw
            by_type[item.source]["stations"].append(code)
        total = sum(item["capacity_mw"] for item in by_type.values())
        return {"station": station, "reachable_sources": sources,
                "total_capacity_mw": total,
                "by_type": {key: {"capacity_mw": round(value["capacity_mw"], 3),
                                  "share": round(value["capacity_mw"] / total, 6) if total else 0,
                                  "stations": sorted(value["stations"])}
                            for key, value in sorted(by_type.items())}}

    def affected_zone_summary(self, incident_id: int) -> dict:
        incident = self.store.incident(incident_id)
        network = self.store.network()
        by_zone = defaultdict(lambda: {"stations": [], "households": 0})
        for code in incident["affected_stations"]:
            item = network.stations[code]
            by_zone[item.zone]["stations"].append(code)
            by_zone[item.zone]["households"] += item.households
        return {"incident_id": incident_id,
                "zones": {zone: {"stations": sorted(value["stations"]),
                                 "households": value["households"]}
                          for zone, value in sorted(by_zone.items())},
                "total_households": incident["affected_households"]}
