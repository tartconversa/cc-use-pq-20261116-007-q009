import unittest
from decimal import Decimal

from heatwatch.contracts import DomainError, Observation, Rule, number, required_text, utc_time
from heatwatch.quality import QualityGate, QualityPolicy
from heatwatch.topology import Network, Pipe, Station


def raw_observation(sequence=1, **changes):
    result = {"station": "b", "sensor": "sensor-b", "sequence": sequence,
              "observed_at": "2026-11-16T08:00:00+08:00",
              "supply_c": 70, "return_c": 50, "flow_m3h": 20,
              "pressure_kpa": 180, "gas_m3h": 3, "source_id": f"record-{sequence}"}
    result.update(changes)
    return result


def station(code, households=10):
    return Station.parse({"code": code, "zone": "north", "households": households,
                          "source": "gas", "capacity_mw": 20})


def pipe(code, upstream, downstream, status="open"):
    return Pipe.parse({"code": code, "upstream": upstream, "downstream": downstream,
                       "max_flow_m3h": 30, "status": status})


class ContractsTest(unittest.TestCase):
    def test_observation_normalizes_timezone(self):
        item = Observation.parse(raw_observation())
        self.assertEqual(item.observed_at, "2026-11-16T00:00:00+00:00")
        self.assertEqual(item.supply_c, Decimal("70"))

    def test_observation_rejects_boolean_sequence(self):
        with self.assertRaises(DomainError) as caught:
            Observation.parse(raw_observation(sequence=True))
        self.assertEqual(caught.exception.code, "invalid_sequence")

    def test_observation_requires_timezone(self):
        with self.assertRaises(DomainError) as caught:
            Observation.parse(raw_observation(observed_at="2026-11-16T08:00:00"))
        self.assertEqual(caught.exception.code, "invalid_time")

    def test_observation_rejects_reverse_temperature(self):
        with self.assertRaises(DomainError) as caught:
            Observation.parse(raw_observation(supply_c=40))
        self.assertEqual(caught.exception.code, "temperature_order")

    def test_observation_rejects_infinite_pressure(self):
        with self.assertRaises(DomainError) as caught:
            Observation.parse(raw_observation(pressure_kpa="Infinity"))
        self.assertEqual(caught.exception.code, "invalid_number")

    def test_observation_rejects_unknown_metric(self):
        item = Observation.parse(raw_observation())
        with self.assertRaises(DomainError) as caught:
            item.metric("missing")
        self.assertEqual(caught.exception.code, "invalid_metric")

    def test_rule_threshold_is_strict(self):
        rule = Rule.parse({"name": "low", "metric": "pressure_kpa", "operator": "lt",
                           "threshold": 180, "severity": "critical"})
        self.assertFalse(rule.breaches(Observation.parse(raw_observation())))
        self.assertTrue(rule.breaches(Observation.parse(raw_observation(pressure_kpa=179))))

    def test_rule_delta_temperature(self):
        rule = Rule.parse({"name": "delta", "metric": "delta_c", "operator": "lte",
                           "threshold": 20, "severity": "warning"})
        self.assertTrue(rule.breaches(Observation.parse(raw_observation())))
        self.assertFalse(rule.breaches(Observation.parse(raw_observation(supply_c=71))))

    def test_rule_rejects_unsupported_operator(self):
        with self.assertRaises(DomainError):
            Rule.parse({"name": "bad", "metric": "pressure_kpa", "operator": "==",
                        "threshold": 180, "severity": "warning"})

    def test_rule_rejects_out_of_range_streak(self):
        with self.assertRaises(DomainError):
            Rule.parse({"name": "bad", "metric": "pressure_kpa", "operator": "lt",
                        "threshold": 180, "severity": "warning", "consecutive": 0})

    def test_required_text_trims_and_bounds(self):
        self.assertEqual(required_text(" x ", "field"), "x")
        with self.assertRaises(DomainError):
            required_text("x" * 121, "field")

    def test_utc_time_rejects_non_string(self):
        with self.assertRaises(DomainError):
            utc_time(123)

    def test_number_excludes_boolean_and_out_of_bounds(self):
        with self.assertRaises(DomainError):
            number(True, "x", "0", "10")
        with self.assertRaises(DomainError):
            number(11, "x", "0", "10")


class TopologyTest(unittest.TestCase):
    def test_downstream_exposure_respects_closed_pipe(self):
        network = Network([station("a"), station("b"), station("c")],
                          [pipe("ab", "a", "b"), pipe("bc", "b", "c", "closed")])
        self.assertEqual(network.affected("a"), ["a", "b"])
        self.assertEqual(network.impacted_households("a"), 20)

    def test_upstream_sources_respect_closed_pipe(self):
        network = Network([station("a"), station("b"), station("c")],
                          [pipe("ab", "a", "b"), pipe("bc", "b", "c", "closed")])
        self.assertEqual(network.reachable_sources("c"), ["c"])

    def test_cycle_is_rejected(self):
        with self.assertRaises(DomainError) as caught:
            Network([station("a"), station("b")],
                    [pipe("ab", "a", "b"), pipe("ba", "b", "a")])
        self.assertEqual(caught.exception.code, "topology_cycle")

    def test_unknown_endpoint_is_rejected(self):
        with self.assertRaises(DomainError):
            Network([station("a")], [pipe("ab", "a", "b")])

    def test_duplicate_station_is_rejected(self):
        with self.assertRaises(DomainError):
            Network([station("a"), station("a")], [])

    def test_self_pipe_is_rejected(self):
        with self.assertRaises(DomainError):
            pipe("aa", "a", "a")

    def test_station_capacity_must_be_finite(self):
        with self.assertRaises(DomainError):
            Station.parse({"code": "a", "zone": "north", "households": 10,
                           "source": "gas", "capacity_mw": float("nan")})

    def test_pipe_capacity_must_be_positive(self):
        with self.assertRaises(DomainError):
            Pipe.parse({"code": "ab", "upstream": "a", "downstream": "b",
                        "max_flow_m3h": 0})


class QualityTest(unittest.TestCase):
    def test_accepted_observation(self):
        result = QualityGate(QualityPolicy()).assess(Observation.parse(raw_observation()), None,
                                                      "2026-11-16T08:01:00+08:00")
        self.assertTrue(result.accepted)
        self.assertEqual(result.reasons, ())

    def test_late_and_future_are_distinct(self):
        gate = QualityGate(QualityPolicy(max_lag_minutes=10, max_future_minutes=1))
        item = Observation.parse(raw_observation())
        self.assertIn("late", gate.assess(item, None, "2026-11-16T08:20:00+08:00").reasons)
        self.assertIn("future", gate.assess(item, None, "2026-11-16T07:00:00+08:00").reasons)

    def test_temperature_jump_is_rejected(self):
        gate = QualityGate(QualityPolicy())
        previous = Observation.parse(raw_observation())
        current = Observation.parse(raw_observation(sequence=2, source_id="next",
                                                    observed_at="2026-11-16T08:05:00+08:00",
                                                    supply_c=110))
        self.assertIn("supply_jump", gate.assess(current, previous,
                                                 "2026-11-16T08:06:00+08:00").reasons)

    def test_pressure_jump_is_rejected(self):
        gate = QualityGate(QualityPolicy(max_pressure_jump_kpa=10))
        previous = Observation.parse(raw_observation())
        current = Observation.parse(raw_observation(sequence=2, source_id="next",
                                                    observed_at="2026-11-16T08:05:00+08:00",
                                                    pressure_kpa=100))
        self.assertIn("pressure_jump", gate.assess(current, previous,
                                                   "2026-11-16T08:06:00+08:00").reasons)

    def test_quality_policy_rejects_negative_jump(self):
        with self.assertRaises(DomainError):
            QualityPolicy.parse({"max_pressure_jump_kpa": -1})

    def test_batch_does_not_advance_on_rejection(self):
        gate = QualityGate(QualityPolicy(max_future_minutes=5))
        first = Observation.parse(raw_observation())
        second = Observation.parse(raw_observation(sequence=2, source_id="next",
                                                   observed_at="2026-11-16T09:00:00+08:00"))
        third = Observation.parse(raw_observation(sequence=3, source_id="third",
                                                  observed_at="2026-11-16T08:10:00+08:00"))
        outcomes = gate.assess_batch([first, second, third], None,
                                     "2026-11-16T08:11:00+08:00")
        self.assertEqual([item.accepted for item in outcomes], [True, False, True])
