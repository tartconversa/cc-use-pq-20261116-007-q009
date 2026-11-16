from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .contracts import DomainError, number, required_text
from .storage import Store
from .topology import Network


@dataclass(frozen=True)
class Demand:
    station: str
    required_mw: Decimal
    priority: int

    @classmethod
    def parse(cls, data: dict) -> "Demand":
        if not isinstance(data, dict):
            raise DomainError("invalid_demand", "demand must be an object")
        priority = data.get("priority", 1)
        if type(priority) is not int or not 1 <= priority <= 5:
            raise DomainError("invalid_priority", "priority must be between 1 and 5")
        return cls(required_text(data.get("station"), "station"),
                   number(data.get("required_mw"), "required_mw", "0", "100000"), priority)

    def as_dict(self) -> dict:
        return {"station": self.station, "required_mw": float(self.required_mw),
                "priority": self.priority}


@dataclass(frozen=True)
class Allocation:
    station: str
    source: str
    mw: Decimal
    path: tuple[str, ...]

    def as_dict(self) -> dict:
        return {"station": self.station, "source": self.source,
                "mw": float(self.mw), "path": list(self.path)}


class CapacityPlanner:
    def __init__(self, network: Network):
        self.network = network

    def _candidate_paths(self, destination: str) -> list[tuple[str, ...]]:
        if destination not in self.network.stations:
            raise DomainError("unknown_station", f"unknown station: {destination}")
        paths = []
        frontier = [(destination, (destination,))]
        while frontier:
            current, path = frontier.pop(0)
            station = self.network.stations[current]
            if station.active and station.capacity_mw > 0:
                paths.append(path[::-1])
            for pipe in self.network.pipes.values():
                if pipe.downstream == current and pipe.status == "open" and pipe.upstream not in path:
                    frontier.append((pipe.upstream, path + (pipe.upstream,)))
        return sorted(paths, key=lambda p: (len(p), p))

    def plan(self, raw_demands: list[dict], *, reserve_fraction: float = 0) -> dict:
        if not isinstance(raw_demands, list) or not raw_demands:
            raise DomainError("invalid_demand", "at least one demand is required")
        demands = [Demand.parse(item) for item in raw_demands]
        if len({item.station for item in demands}) != len(demands):
            raise DomainError("duplicate_demand", "station demand is repeated")
        reserve = number(reserve_fraction, "reserve_fraction", "0", "0.9")
        remaining = {code: Decimal(str(station.capacity_mw)) * (1 - reserve)
                     for code, station in self.network.stations.items() if station.active}
        pipe_remaining = {code: Decimal(str(pipe.max_flow_m3h)) * Decimal("1.163")
                          * Decimal("20") / Decimal("1000")
                          for code, pipe in self.network.pipes.items() if pipe.status == "open"}
        allocations: list[Allocation] = []
        unmet = []
        for demand in sorted(demands, key=lambda d: (-d.priority, d.station)):
            needed = demand.required_mw
            for path in self._candidate_paths(demand.station):
                if needed <= 0:
                    break
                source = path[0]
                pipe_codes = []
                for upstream, downstream in zip(path, path[1:]):
                    matches = [pipe.code for pipe in self.network.pipes.values()
                               if pipe.upstream == upstream and pipe.downstream == downstream
                               and pipe.status == "open"]
                    if not matches:
                        raise DomainError("network_inconsistent", "missing path pipe")
                    pipe_codes.append(sorted(matches)[0])
                available = min([remaining[source]] + [pipe_remaining[code] for code in pipe_codes])
                provided = min(needed, available)
                if provided <= 0:
                    continue
                remaining[source] -= provided
                for code in pipe_codes:
                    pipe_remaining[code] -= provided
                allocations.append(Allocation(demand.station, source, provided, path))
                needed -= provided
            if needed > 0:
                unmet.append({"station": demand.station, "unmet_mw": float(needed),
                              "priority": demand.priority,
                              "households": self.network.stations[demand.station].households})
        return {"allocations": [item.as_dict() for item in allocations],
                "unmet": unmet, "feasible": not unmet,
                "remaining_mw": {code: float(value) for code, value in sorted(remaining.items())},
                "pipe_remaining_mw": {code: float(value) for code, value in sorted(pipe_remaining.items())},
                "assumed_pipe_delta_c": 20,
                "reserve_fraction": float(reserve)}


class HeatBalance:
    def __init__(self, store: Store):
        self.store = store

    def latest(self, station: str, *, sensor: str | None = None) -> dict:
        network = self.store.network()
        if station not in network.stations:
            raise DomainError("unknown_station", "station does not exist")
        query = "SELECT * FROM observations WHERE station=? AND accepted=1"
        args = [station]
        if sensor is not None:
            query += " AND sensor=?"
            args.append(required_text(sensor, "sensor"))
        query += " ORDER BY observed_at DESC,id DESC LIMIT 1"
        row = self.store.connection.execute(query, args).fetchone()
        if row is None:
            return {"station": station, "available": False, "reason": "no_accepted_observation"}
        supply = Decimal(row["supply_c"])
        returning = Decimal(row["return_c"])
        flow = Decimal(row["flow_m3h"])
        thermal_mw = flow * (supply - returning) * Decimal("1.163") / Decimal("1000")
        return {"station": station, "available": True,
                "observed_at": row["observed_at"], "sensor": row["sensor"],
                "delta_c": float(supply - returning), "flow_m3h": float(flow),
                "thermal_mw": round(float(thermal_mw), 6),
                "source": network.stations[station].source,
                "capacity_mw": network.stations[station].capacity_mw,
                "utilization": round(float(thermal_mw) / network.stations[station].capacity_mw, 6)}

    def zone_summary(self, zone: str) -> dict:
        zone = required_text(zone, "zone")
        network = self.store.network()
        members = [station for station in network.stations.values() if station.zone == zone]
        if not members:
            raise DomainError("unknown_zone", "zone does not exist")
        measurements = [self.latest(station.code) for station in members]
        available = [item for item in measurements if item["available"]]
        return {"zone": zone, "stations": measurements,
                "coverage": len(available) / len(members),
                "thermal_mw": round(sum(item["thermal_mw"] for item in available), 6),
                "capacity_mw": sum(station.capacity_mw for station in members),
                "households": sum(station.households for station in members),
                "open_incidents": len([item for item in self.store.incidents("open")
                                       if network.stations[item["station"]].zone == zone])}
