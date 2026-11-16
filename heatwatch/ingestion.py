from __future__ import annotations

import hashlib
import json

from .alerts import AlertEngine
from .contracts import DomainError, Observation, required_text, utc_time
from .quality import QualityGate, QualityPolicy
from .storage import Store


class IngestionService:
    def __init__(self, store: Store):
        self.store = store

    def ingest(self, batch_id: str, raw_observations: list[dict],
               received_at: str) -> dict:
        batch_id = required_text(batch_id, "batch_id", 200)
        received_at = utc_time(received_at, "received_at")
        if not isinstance(raw_observations, list) or not 1 <= len(raw_observations) <= 10000:
            raise DomainError("invalid_batch", "batch must have between 1 and 10000 observations")
        observations = [Observation.parse(item) for item in raw_observations]
        source_ids = [item.source_id for item in observations]
        if len(set(source_ids)) != len(source_ids):
            raise DomainError("duplicate_source", "source_id repeats within batch")
        digest = hashlib.sha256(json.dumps([item.as_dict() for item in observations],
                                          sort_keys=True).encode()).hexdigest()
        with self.store.transaction():
            existing = self.store.batch(batch_id)
            if existing:
                if existing["digest"] != digest:
                    raise DomainError("batch_conflict", "batch_id already has different content")
                return {"batch_id": batch_id, "status": "duplicate",
                        "accepted": existing["accepted"], "rejected": existing["rejected"],
                        "transitions": []}
            network = self.store.network()
            if not network.stations:
                raise DomainError("network_missing", "configure stations before ingesting")
            for item in observations:
                station = network.stations.get(item.station)
                if station is None:
                    raise DomainError("unknown_station", f"unknown station: {item.station}")
                if not station.active:
                    raise DomainError("inactive_station", f"station is inactive: {item.station}")
                prior = self.store.observation_by_source(item.source_id)
                if prior:
                    raise DomainError("source_conflict", "source_id already exists",
                                      details={"source_id": item.source_id})
            policy_data = self.store.config("quality_policy")
            policy = QualityPolicy.parse(policy_data["value"] if policy_data else {})
            gate = QualityGate(policy)
            engine = AlertEngine(self.store, network)
            accepted = rejected = 0
            transitions = []
            self.store.add_batch(batch_id, digest, received_at, 0, 0, "processing")
            latest = {}
            for item in observations:
                previous = latest.get(item.sensor)
                if previous is None:
                    previous = self.store.latest_accepted(item.sensor)
                if previous and previous.station != item.station:
                    raise DomainError("sensor_moved", "sensor cannot silently change station")
                result = gate.assess(item, previous, received_at)
                observation_id = self.store.add_observation(item, batch_id, result.accepted,
                                                            list(result.reasons), received_at)
                if result.accepted:
                    accepted += 1
                    latest[item.sensor] = item
                    transitions.extend(engine.evaluate(item, observation_id))
                else:
                    rejected += 1
            status = "accepted" if not rejected else "partial" if accepted else "rejected"
            self.store.connection.execute("""UPDATE batches SET accepted=?,rejected=?,status=?
                WHERE batch_id=?""", (accepted, rejected, status, batch_id))
            return {"batch_id": batch_id, "status": status, "accepted": accepted,
                    "rejected": rejected, "transitions": transitions}

    def replay(self, observations: list[dict], received_at: str) -> dict:
        parsed = [Observation.parse(item) for item in observations]
        network = self.store.network()
        policy_data = self.store.config("quality_policy")
        gate = QualityGate(QualityPolicy.parse(policy_data["value"] if policy_data else {}))
        outcomes, latest = [], {}
        for item in parsed:
            if item.station not in network.stations:
                raise DomainError("unknown_station", f"unknown station: {item.station}")
            previous = latest.get(item.sensor) or self.store.latest_accepted(item.sensor)
            result = gate.assess(item, previous, received_at)
            outcomes.append({"source_id": item.source_id, **result.as_dict()})
            if result.accepted:
                latest[item.sensor] = item
        return {"observations": outcomes, "accepted": sum(item["accepted"] for item in outcomes)}
