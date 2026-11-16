from __future__ import annotations

from .alerts import AlertEngine
from .capacity import CapacityPlanner, HeatBalance
from .diagnostics import DiagnosticService
from .contracts import DomainError, Rule
from .dispatch import DispatchService
from .ingestion import IngestionService
from .operations import OperationsService
from .quality import QualityPolicy
from .reporting import ReportingService
from .response import ResponsePlanner
from .storage import Store
from .topology import Network, Pipe, Station


class HeatService:
    def __init__(self, db_path: str):
        self.store = Store(db_path)
        self.ingestion = IngestionService(self.store)
        self.dispatch = DispatchService(self.store)
        self.reporting = ReportingService(self.store)
        self.operations = OperationsService(self.store)
        self.diagnostics = DiagnosticService(self.store)
        self.response = ResponsePlanner(self.store)

    def close(self) -> None:
        self.store.close()

    def configure_network(self, stations: list[dict], pipes: list[dict],
                          expected_revision: int | None = None) -> dict:
        network = Network([Station.parse(item) for item in stations],
                          [Pipe.parse(item) for item in pipes])
        with self.store.transaction():
            current = self.store.config("network_revision")
            previous = current["revision"] if current else 0
            if expected_revision is not None and expected_revision != previous:
                raise DomainError("revision_conflict", "network revision changed")
            self.store.replace_network(network)
            revision = self.store.set_config("network_revision", network.as_dict(), previous)
            engine = AlertEngine(self.store, network)
            revised = [engine.reproject_exposure(station) for station in network.stations]
        return {"revision": revision, "stations": len(network.stations),
                "pipes": len(network.pipes), "reprojected_incidents": sum(map(len, revised))}

    def configure_rules(self, rules: list[dict]) -> dict:
        parsed = [Rule.parse(item) for item in rules]
        with self.store.transaction():
            self.store.replace_rules(parsed)
        return {"rules": [item.as_dict() for item in parsed]}

    def configure_quality(self, policy: dict, expected_revision: int | None = None) -> dict:
        parsed = QualityPolicy.parse(policy)
        with self.store.transaction():
            revision = self.store.set_config("quality_policy", parsed.as_dict(), expected_revision)
        return {"revision": revision, "policy": parsed.as_dict()}

    def status(self) -> dict:
        network = self.store.network()
        return {"stations": len(network.stations), "pipes": len(network.pipes),
                "rules": len(self.store.rules()),
                "batches": self.store.connection.execute("SELECT COUNT(*) FROM batches").fetchone()[0],
                "open_incidents": len(self.store.incidents("open")),
                "reconciliation": self.reporting.reconcile()}

    def incident_detail(self, incident_id: int) -> dict:
        return {"incident": self.store.incident(incident_id),
                "events": self.store.events(incident_id),
                "dispatches": self.dispatch.for_incident(incident_id)}

    def timeline(self, station: str) -> dict:
        network = self.store.network()
        if station not in network.stations:
            raise DomainError("unknown_station", "unknown station")
        return {"station": station, "exposure": network.exposure(station),
                "observations": self.store.observations(station=station),
                "states": AlertEngine(self.store, network).state_snapshot(station),
                "incidents": [i for i in self.store.incidents() if i["station"] == station]}

    def plan_capacity(self, demands: list[dict], reserve_fraction: float = 0) -> dict:
        return CapacityPlanner(self.store.network()).plan(demands,
                                                           reserve_fraction=reserve_fraction)

    def heat_balance(self, station: str) -> dict:
        return HeatBalance(self.store).latest(station)

    def zone_summary(self, zone: str) -> dict:
        return HeatBalance(self.store).zone_summary(zone)
