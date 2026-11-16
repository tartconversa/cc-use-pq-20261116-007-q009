from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from .contracts import DomainError, required_text


@dataclass(frozen=True)
class Station:
    code: str
    zone: str
    households: int
    source: str
    capacity_mw: float
    active: bool = True

    @classmethod
    def parse(cls, data: dict) -> "Station":
        if not isinstance(data, dict):
            raise DomainError("invalid_station", "station must be an object")
        code = required_text(data.get("code"), "code")
        zone = required_text(data.get("zone"), "zone")
        source = data.get("source")
        if source not in {"gas", "waste_heat", "electricity", "storage", "nuclear_heat"}:
            raise DomainError("invalid_source", "unsupported heat source")
        households = data.get("households")
        if type(households) is not int or households < 0:
            raise DomainError("invalid_households", "households must be a nonnegative integer")
        capacity = data.get("capacity_mw")
        if isinstance(capacity, bool) or not isinstance(capacity, (int, float)) or not isfinite(capacity) or not 0 < capacity <= 100000:
            raise DomainError("invalid_capacity", "capacity_mw must be positive and finite")
        active = data.get("active", True)
        if type(active) is not bool:
            raise DomainError("invalid_active", "active must be boolean")
        return cls(code, zone, households, source, float(capacity), active)

    def as_dict(self) -> dict:
        return vars(self).copy()


@dataclass(frozen=True)
class Pipe:
    code: str
    upstream: str
    downstream: str
    max_flow_m3h: float
    status: str = "open"

    @classmethod
    def parse(cls, data: dict) -> "Pipe":
        if not isinstance(data, dict):
            raise DomainError("invalid_pipe", "pipe must be an object")
        code = required_text(data.get("code"), "code")
        upstream = required_text(data.get("upstream"), "upstream")
        downstream = required_text(data.get("downstream"), "downstream")
        if upstream == downstream:
            raise DomainError("pipe_loop", "pipe cannot connect a station to itself")
        capacity = data.get("max_flow_m3h")
        if isinstance(capacity, bool) or not isinstance(capacity, (int, float)) or not isfinite(capacity) or not 0 < capacity <= 100000:
            raise DomainError("invalid_capacity", "max_flow_m3h must be positive")
        status = data.get("status", "open")
        if status not in {"open", "closed"}:
            raise DomainError("invalid_status", "pipe status must be open or closed")
        return cls(code, upstream, downstream, float(capacity), status)

    def as_dict(self) -> dict:
        return vars(self).copy()


class Network:
    def __init__(self, stations: list[Station], pipes: list[Pipe]):
        self.stations = {item.code: item for item in stations}
        self.pipes = {item.code: item for item in pipes}
        if len(self.stations) != len(stations) or len(self.pipes) != len(pipes):
            raise DomainError("duplicate_topology", "station or pipe code is duplicated")
        for pipe in pipes:
            if pipe.upstream not in self.stations or pipe.downstream not in self.stations:
                raise DomainError("unknown_endpoint", "pipe references unknown station", details={"pipe": pipe.code})
        self._validate_dag()

    def _validate_dag(self) -> None:
        graph = {code: [] for code in self.stations}
        for pipe in self.pipes.values():
            graph[pipe.upstream].append(pipe.downstream)
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(code: str) -> None:
            if code in visiting:
                raise DomainError("topology_cycle", "heat network contains a cycle")
            if code in visited:
                return
            visiting.add(code)
            for next_code in graph[code]:
                visit(next_code)
            visiting.remove(code)
            visited.add(code)

        for code in graph:
            visit(code)

    def affected(self, station: str) -> list[str]:
        if station not in self.stations:
            raise DomainError("unknown_station", f"unknown station: {station}")
        result = {station}
        frontier = [station]
        while frontier:
            current = frontier.pop()
            for pipe in self.pipes.values():
                if pipe.upstream == current and pipe.status == "open" and pipe.downstream not in result:
                    result.add(pipe.downstream)
                    frontier.append(pipe.downstream)
        return sorted(result)

    def impacted_households(self, station: str) -> int:
        return sum(self.stations[code].households for code in self.affected(station))

    def reachable_sources(self, station: str) -> list[str]:
        if station not in self.stations:
            raise DomainError("unknown_station", f"unknown station: {station}")
        found = {station}
        frontier = [station]
        while frontier:
            current = frontier.pop()
            for pipe in self.pipes.values():
                if pipe.downstream == current and pipe.status == "open" and pipe.upstream not in found:
                    found.add(pipe.upstream)
                    frontier.append(pipe.upstream)
        return sorted(code for code in found if self.stations[code].active)

    def exposure(self, station: str) -> dict:
        affected = self.affected(station)
        return {"stations": affected, "households": self.impacted_households(station),
                "sources": self.reachable_sources(station)}

    def as_dict(self) -> dict:
        return {"stations": [s.as_dict() for s in self.stations.values()],
                "pipes": [p.as_dict() for p in self.pipes.values()]}
