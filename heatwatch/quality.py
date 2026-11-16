from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from .contracts import DomainError, Observation, utc_time


@dataclass(frozen=True)
class QualityPolicy:
    max_lag_minutes: int = 120
    max_future_minutes: int = 5
    max_temperature_jump_c: Decimal = Decimal("35")
    max_pressure_jump_kpa: Decimal = Decimal("1000")
    max_flow_jump_m3h: Decimal = Decimal("5000")
    min_delta_c: Decimal = Decimal("0")

    @classmethod
    def parse(cls, data: dict) -> "QualityPolicy":
        if not isinstance(data, dict):
            raise DomainError("invalid_policy", "quality policy must be an object")
        lag = data.get("max_lag_minutes", 120)
        future = data.get("max_future_minutes", 5)
        for name, value in (("max_lag_minutes", lag), ("max_future_minutes", future)):
            if type(value) is not int or not 0 <= value <= 10080:
                raise DomainError("invalid_policy", f"{name} must be a bounded integer")
        jumps = []
        for name, default in (("max_temperature_jump_c", "35"),
                              ("max_pressure_jump_kpa", "1000"),
                              ("max_flow_jump_m3h", "5000")):
            try:
                value = Decimal(str(data.get(name, default)))
            except Exception as exc:
                raise DomainError("invalid_policy", f"{name} is invalid") from exc
            if not value.is_finite() or value < 0:
                raise DomainError("invalid_policy", f"{name} must be nonnegative")
            jumps.append(value)
        return cls(lag, future, *jumps)

    def as_dict(self) -> dict:
        return {"max_lag_minutes": self.max_lag_minutes,
                "max_future_minutes": self.max_future_minutes,
                "max_temperature_jump_c": float(self.max_temperature_jump_c),
                "max_pressure_jump_kpa": float(self.max_pressure_jump_kpa),
                "max_flow_jump_m3h": float(self.max_flow_jump_m3h)}


@dataclass(frozen=True)
class QualityResult:
    accepted: bool
    reasons: tuple[str, ...]
    observed_at: str
    previous_at: str | None

    def as_dict(self) -> dict:
        return {"accepted": self.accepted, "reasons": list(self.reasons),
                "observed_at": self.observed_at, "previous_at": self.previous_at}


class QualityGate:
    def __init__(self, policy: QualityPolicy):
        self.policy = policy

    def assess(self, observation: Observation, previous: Observation | None,
               received_at: str) -> QualityResult:
        now = datetime.fromisoformat(utc_time(received_at, "received_at"))
        when = datetime.fromisoformat(observation.observed_at)
        reasons = []
        if when < now - timedelta(minutes=self.policy.max_lag_minutes):
            reasons.append("late")
        if when > now + timedelta(minutes=self.policy.max_future_minutes):
            reasons.append("future")
        if previous:
            before = datetime.fromisoformat(previous.observed_at)
            if observation.sequence <= previous.sequence:
                reasons.append("out_of_order_sequence")
            if when <= before:
                reasons.append("out_of_order_time")
            if abs(observation.supply_c - previous.supply_c) > self.policy.max_temperature_jump_c:
                reasons.append("supply_jump")
            if abs(observation.return_c - previous.return_c) > self.policy.max_temperature_jump_c:
                reasons.append("return_jump")
            if abs(observation.pressure_kpa - previous.pressure_kpa) > self.policy.max_pressure_jump_kpa:
                reasons.append("pressure_jump")
            if abs(observation.flow_m3h - previous.flow_m3h) > self.policy.max_flow_jump_m3h:
                reasons.append("flow_jump")
        if observation.supply_c - observation.return_c < self.policy.min_delta_c:
            reasons.append("temperature_delta")
        return QualityResult(not reasons, tuple(reasons), observation.observed_at,
                             previous.observed_at if previous else None)

    def assess_batch(self, observations: list[Observation], previous: Observation | None,
                     received_at: str) -> list[QualityResult]:
        results = []
        current = previous
        for observation in observations:
            result = self.assess(observation, current, received_at)
            results.append(result)
            if result.accepted:
                current = observation
        return results
