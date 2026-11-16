from __future__ import annotations

from contextlib import contextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .contracts import DomainError
from .service import HeatService


class NetworkPayload(BaseModel):
    stations: list[dict]
    pipes: list[dict]
    expected_revision: int | None = None


class RulesPayload(BaseModel):
    rules: list[dict]


class QualityPayload(BaseModel):
    policy: dict
    expected_revision: int | None = None


class BatchPayload(BaseModel):
    batch_id: str
    observations: list[dict]
    received_at: str


class DispatchPayload(BaseModel):
    team: str
    at: str
    actor: str


class TransitionPayload(BaseModel):
    status: str
    expected_revision: int
    at: str
    actor: str


class ExportPayload(BaseModel):
    from_at: str
    to_at: str
    created_at: str


class CapacityPayload(BaseModel):
    demands: list[dict]
    reserve_fraction: float = 0


def create_app(db_path: str = "heatwatch.sqlite3") -> FastAPI:
    app = FastAPI(title="HeatWatch", version="1.0.0")

    @contextmanager
    def service():
        instance = HeatService(db_path)
        try:
            yield instance
        finally:
            instance.close()

    @app.exception_handler(DomainError)
    async def domain_error(_request, error: DomainError):
        status = 409 if error.code.endswith("conflict") else 404 if error.code.startswith("unknown_") else 422
        return JSONResponse(status_code=status, content=error.as_dict())

    @app.get("/status")
    def status():
        with service() as instance:
            return instance.status()

    @app.put("/network")
    def configure_network(payload: NetworkPayload):
        with service() as instance:
            return instance.configure_network(payload.stations, payload.pipes,
                                              payload.expected_revision)

    @app.get("/network")
    def get_network():
        with service() as instance:
            return instance.store.network().as_dict()

    @app.put("/rules")
    def configure_rules(payload: RulesPayload):
        with service() as instance:
            return instance.configure_rules(payload.rules)

    @app.get("/rules")
    def rules():
        with service() as instance:
            return [rule.as_dict() for rule in instance.store.rules()]

    @app.put("/quality")
    def configure_quality(payload: QualityPayload):
        with service() as instance:
            return instance.configure_quality(payload.policy, payload.expected_revision)

    @app.post("/batches")
    def ingest(payload: BatchPayload):
        with service() as instance:
            return instance.ingestion.ingest(payload.batch_id, payload.observations,
                                             payload.received_at)

    @app.post("/replay")
    def replay(payload: BatchPayload):
        with service() as instance:
            return instance.ingestion.replay(payload.observations, payload.received_at)

    @app.get("/stations/{station}/timeline")
    def timeline(station: str):
        with service() as instance:
            return instance.timeline(station)

    @app.get("/incidents")
    def incidents(status: str | None = None):
        with service() as instance:
            return instance.store.incidents(status)

    @app.get("/incidents/{incident_id}")
    def incident(incident_id: int):
        with service() as instance:
            return instance.incident_detail(incident_id)

    @app.post("/incidents/{incident_id}/dispatches")
    def assign(incident_id: int, payload: DispatchPayload):
        with service() as instance:
            return instance.dispatch.assign(incident_id, payload.team, payload.at, payload.actor)

    @app.patch("/dispatches/{dispatch_id}")
    def transition(dispatch_id: int, payload: TransitionPayload):
        with service() as instance:
            return instance.dispatch.transition(dispatch_id, payload.status,
                                                payload.expected_revision, payload.at,
                                                payload.actor)

    @app.get("/reports/window")
    def report(from_at: str, to_at: str):
        with service() as instance:
            return instance.reporting.window(from_at, to_at)

    @app.get("/reports/reconcile")
    def reconcile():
        with service() as instance:
            return instance.reporting.reconcile()

    @app.post("/exports")
    def export(payload: ExportPayload):
        with service() as instance:
            return instance.reporting.export(payload.from_at, payload.to_at, payload.created_at)

    @app.get("/exports/{export_id}")
    def saved_export(export_id: int):
        with service() as instance:
            return instance.reporting.saved_export(export_id)

    @app.post("/capacity/plan")
    def plan_capacity(payload: CapacityPayload):
        with service() as instance:
            return instance.plan_capacity(payload.demands, payload.reserve_fraction)

    @app.get("/stations/{station}/balance")
    def heat_balance(station: str):
        with service() as instance:
            return instance.heat_balance(station)

    @app.get("/zones/{zone}/summary")
    def zone_summary(zone: str):
        with service() as instance:
            return instance.zone_summary(zone)

    @app.get("/operations/freshness")
    def freshness(now: str, allowed_minutes: int = 30):
        with service() as instance:
            return instance.operations.freshness(now, allowed_minutes)

    @app.get("/operations/sensors/{sensor}")
    def sensor_history(sensor: str):
        with service() as instance:
            return instance.operations.sensor_history(sensor)

    @app.get("/operations/workload")
    def workload(from_at: str, to_at: str):
        with service() as instance:
            return instance.operations.incident_workload(from_at, to_at)

    @app.get("/operations/audit")
    def audit():
        with service() as instance:
            return instance.operations.audit()

    @app.get("/diagnostics/quality")
    def quality_diagnostics(from_at: str, to_at: str):
        with service() as instance:
            return instance.diagnostics.batch_quality(from_at, to_at)

    @app.get("/diagnostics/gaps/{sensor}")
    def gaps(sensor: str, from_sequence: int = 0):
        with service() as instance:
            return instance.diagnostics.gaps(sensor, from_sequence)

    @app.get("/diagnostics/continuity/{station}")
    def continuity(station: str, from_at: str, to_at: str, expected_minutes: int = 15):
        with service() as instance:
            return instance.diagnostics.station_continuity(station, from_at, to_at,
                                                           expected_minutes)

    @app.get("/diagnostics/evidence/{incident_id}")
    def evidence(incident_id: int):
        with service() as instance:
            return instance.diagnostics.incident_evidence(incident_id)

    @app.get("/diagnostics/risk")
    def risk():
        with service() as instance:
            return instance.diagnostics.open_risk_by_zone()

    @app.get("/response/queue")
    def response_queue(now: str):
        with service() as instance:
            return instance.response.queue(now)

    @app.get("/response/incidents/{incident_id}/zones")
    def affected_zones(incident_id: int):
        with service() as instance:
            return instance.response.affected_zone_summary(incident_id)

    @app.get("/response/stations/{station}/sources")
    def source_mix(station: str):
        with service() as instance:
            return instance.response.station_source_mix(station)

    @app.post("/response/pipes/{pipe_code}/isolation")
    def isolate_pipe(pipe_code: str, payload: CapacityPayload):
        with service() as instance:
            return instance.response.isolate_pipe(pipe_code, payload.demands,
                                                  payload.reserve_fraction)

    return app
