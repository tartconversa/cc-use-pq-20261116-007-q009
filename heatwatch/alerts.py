from __future__ import annotations

from .contracts import Observation, Rule
from .storage import Store
from .topology import Network


class AlertEngine:
    def __init__(self, store: Store, network: Network):
        self.store = store
        self.network = network

    def evaluate(self, observation: Observation, observation_id: int) -> list[dict]:
        transitions = []
        for rule in self.store.rules():
            state = self.store.rule_state(observation.station, rule.name)
            if state["last_observation"] == observation_id:
                continue
            breached = rule.breaches(observation)
            if breached:
                state["breach_count"] += 1
                state["clear_count"] = 0
            else:
                state["clear_count"] += 1
                state["breach_count"] = 0
            incident_id = state["active_incident"]
            if incident_id is None and breached and state["breach_count"] >= rule.consecutive:
                exposure = self.network.exposure(observation.station)
                incident_id = self.store.open_incident(observation.station, rule,
                                                       observation_id, observation.observed_at,
                                                       exposure)
                state["active_incident"] = incident_id
                transitions.append({"type": "opened", "incident_id": incident_id,
                                    "station": observation.station, "rule": rule.name})
            elif incident_id is not None and not breached and state["clear_count"] >= rule.clear_consecutive:
                self.store.close_incident(incident_id, observation.observed_at, observation_id)
                state["active_incident"] = None
                transitions.append({"type": "closed", "incident_id": incident_id,
                                    "station": observation.station, "rule": rule.name})
            elif incident_id is not None:
                self.store.touch_incident(incident_id, observation_id)
            state["last_observation"] = observation_id
            self.store.save_rule_state(state)
        return transitions

    def state_snapshot(self, station: str | None = None) -> list[dict]:
        query = "SELECT * FROM rule_state"
        arguments = ()
        if station:
            query += " WHERE station=?"
            arguments = (station,)
        query += " ORDER BY station,rule"
        return [dict(row) for row in self.store.connection.execute(query, arguments)]

    def reproject_exposure(self, station: str) -> list[dict]:
        affected = []
        exposure = self.network.exposure(station)
        for incident in self.store.incidents("open"):
            if incident["station"] != station:
                continue
            if (incident["affected_households"] == exposure["households"]
                    and incident["affected_stations"] == exposure["stations"]):
                continue
            self.store.connection.execute("""UPDATE incidents SET affected_households=?,
                affected_stations=?,revision=revision+1 WHERE id=?""",
                (exposure["households"], __import__("json").dumps(exposure["stations"]),
                 incident["id"]))
            self.store.event(incident["id"], "exposure_changed", incident["opened_at"],
                             "system", {"before": incident["affected_households"],
                                        "after": exposure["households"]})
            affected.append(self.store.incident(incident["id"]))
        return affected
