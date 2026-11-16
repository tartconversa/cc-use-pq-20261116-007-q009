from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


class DomainError(ValueError):
    def __init__(self, code: str, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}

    def as_dict(self) -> dict:
        return {"code": self.code, "message": str(self), "details": self.details}


def required_text(value: Any, field: str, maximum: int = 120) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise DomainError("invalid_text", f"{field} must be nonempty text of at most {maximum} characters")
    return value.strip()


def utc_time(value: Any, field: str = "observed_at") -> str:
    if not isinstance(value, str):
        raise DomainError("invalid_time", f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DomainError("invalid_time", f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise DomainError("invalid_time", f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def number(value: Any, field: str, minimum: str, maximum: str) -> Decimal:
    if isinstance(value, bool):
        raise DomainError("invalid_number", f"{field} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise DomainError("invalid_number", f"{field} must be numeric") from exc
    if not result.is_finite() or result < Decimal(minimum) or result > Decimal(maximum):
        raise DomainError("invalid_number", f"{field} is outside [{minimum}, {maximum}]")
    return result


@dataclass(frozen=True)
class Observation:
    station: str
    sensor: str
    sequence: int
    observed_at: str
    supply_c: Decimal
    return_c: Decimal
    flow_m3h: Decimal
    pressure_kpa: Decimal
    gas_m3h: Decimal
    source_id: str

    @classmethod
    def parse(cls, data: dict) -> "Observation":
        if not isinstance(data, dict):
            raise DomainError("invalid_observation", "observation must be an object")
        sequence = data.get("sequence")
        if type(sequence) is not int or sequence < 0:
            raise DomainError("invalid_sequence", "sequence must be a nonnegative integer")
        supply = number(data.get("supply_c"), "supply_c", "-30", "160")
        returning = number(data.get("return_c"), "return_c", "-30", "160")
        if returning > supply:
            raise DomainError("temperature_order", "return temperature exceeds supply temperature")
        return cls(required_text(data.get("station"), "station"),
                   required_text(data.get("sensor"), "sensor"), sequence,
                   utc_time(data.get("observed_at")), supply, returning,
                   number(data.get("flow_m3h"), "flow_m3h", "0", "100000"),
                   number(data.get("pressure_kpa"), "pressure_kpa", "0", "20000"),
                   number(data.get("gas_m3h", 0), "gas_m3h", "0", "100000"),
                   required_text(data.get("source_id"), "source_id", 200))

    def metric(self, name: str) -> Decimal:
        if name not in {"supply_c", "return_c", "flow_m3h", "pressure_kpa", "gas_m3h"}:
            raise DomainError("invalid_metric", f"unknown metric: {name}")
        return getattr(self, name)

    def as_dict(self) -> dict:
        return {"station": self.station, "sensor": self.sensor, "sequence": self.sequence,
                "observed_at": self.observed_at, "supply_c": float(self.supply_c),
                "return_c": float(self.return_c), "flow_m3h": float(self.flow_m3h),
                "pressure_kpa": float(self.pressure_kpa), "gas_m3h": float(self.gas_m3h),
                "source_id": self.source_id}


@dataclass(frozen=True)
class Rule:
    name: str
    metric: str
    operator: str
    threshold: Decimal
    severity: str
    consecutive: int
    clear_consecutive: int

    @classmethod
    def parse(cls, data: dict) -> "Rule":
        if not isinstance(data, dict):
            raise DomainError("invalid_rule", "rule must be an object")
        name = required_text(data.get("name"), "name")
        metric = required_text(data.get("metric"), "metric")
        if metric not in {"supply_c", "return_c", "flow_m3h", "pressure_kpa", "gas_m3h", "delta_c"}:
            raise DomainError("invalid_metric", f"unknown rule metric: {metric}")
        operator = data.get("operator")
        if operator not in {"lt", "lte", "gt", "gte"}:
            raise DomainError("invalid_operator", "operator must be lt, lte, gt or gte")
        severity = data.get("severity")
        if severity not in {"warning", "critical"}:
            raise DomainError("invalid_severity", "severity must be warning or critical")
        consecutive = data.get("consecutive", 1)
        clear = data.get("clear_consecutive", 1)
        if type(consecutive) is not int or not 1 <= consecutive <= 100:
            raise DomainError("invalid_consecutive", "consecutive must be between 1 and 100")
        if type(clear) is not int or not 1 <= clear <= 100:
            raise DomainError("invalid_consecutive", "clear_consecutive must be between 1 and 100")
        return cls(name, metric, operator,
                   number(data.get("threshold"), "threshold", "-100000", "100000"),
                   severity, consecutive, clear)

    def breaches(self, observation: Observation) -> bool:
        value = observation.supply_c - observation.return_c if self.metric == "delta_c" else observation.metric(self.metric)
        return {"lt": value < self.threshold, "lte": value <= self.threshold,
                "gt": value > self.threshold, "gte": value >= self.threshold}[self.operator]

    def as_dict(self) -> dict:
        return {"name": self.name, "metric": self.metric, "operator": self.operator,
                "threshold": float(self.threshold), "severity": self.severity,
                "consecutive": self.consecutive, "clear_consecutive": self.clear_consecutive}
