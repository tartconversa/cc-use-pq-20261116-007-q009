import tempfile
import unittest
from pathlib import Path

from heatwatch.capacity import CapacityPlanner
from heatwatch.contracts import DomainError
from heatwatch.demo import run
from heatwatch.service import HeatService
from heatwatch.topology import Network, Pipe, Station

from test_service import observation, pipes, rules, stations


class OperationsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.service = HeatService(str(Path(self.temporary.name) / "operations.sqlite3"))
        self.service.configure_network(stations(), pipes())
        self.service.configure_rules(rules())

    def tearDown(self):
        self.service.close()
        self.temporary.cleanup()

    def ingest(self, sequence, pressure=190):
        item = observation(sequence, pressure=pressure)
        return self.service.ingestion.ingest(f"b{sequence}", [item],
                                             f"2026-11-16T08:{sequence*5+1:02d}:00+08:00")

    def test_capacity_allocation_prefers_local_source(self):
        result = self.service.plan_capacity([{"station": "b", "required_mw": 10.5,
                                              "priority": 3}])
        self.assertTrue(result["feasible"])
        self.assertEqual(result["allocations"][0]["source"], "b")
        self.assertEqual(result["allocations"][0]["mw"], 10)
        self.assertEqual(result["allocations"][1]["source"], "a")
        self.assertLess(result["allocations"][1]["mw"], 1)

    def test_capacity_reports_shortfall(self):
        result = self.service.plan_capacity([{"station": "c", "required_mw": 100,
                                              "priority": 5}])
        self.assertFalse(result["feasible"])
        self.assertGreater(result["unmet"][0]["unmet_mw"], 0)
        self.assertEqual(result["unmet"][0]["households"], 200)

    def test_capacity_reserve_reduces_available_heat(self):
        demand = [{"station": "a", "required_mw": 40, "priority": 5}]
        self.assertTrue(self.service.plan_capacity(demand)["feasible"])
        self.assertFalse(self.service.plan_capacity(demand, reserve_fraction=0.5)["feasible"])

    def test_capacity_rejects_duplicate_demand(self):
        demand = {"station": "b", "required_mw": 5, "priority": 1}
        with self.assertRaises(DomainError):
            self.service.plan_capacity([demand, demand])

    def test_capacity_priority_changes_allocation(self):
        result = self.service.plan_capacity([
            {"station": "c", "required_mw": 60, "priority": 5},
            {"station": "b", "required_mw": 40, "priority": 1}])
        self.assertEqual(result["allocations"][0]["station"], "c")

    def test_isolate_pipe_increases_shortfall(self):
        demand = [{"station": "c", "required_mw": 6, "priority": 5}]
        result = self.service.response.isolate_pipe("bc", demand)
        self.assertGreater(result["new_shortfall_mw"], 0)
        self.assertGreater(result["after"]["unmet"][0]["unmet_mw"],
                           result["before"]["unmet"][0]["unmet_mw"])

    def test_isolation_simulation_does_not_change_network(self):
        self.service.response.isolate_pipe("bc", [{"station": "c", "required_mw": 30}])
        self.assertEqual(self.service.store.network().pipes["bc"].status, "open")

    def test_source_mix_uses_upstream_graph(self):
        mix = self.service.response.station_source_mix("c")
        self.assertEqual(mix["reachable_sources"], ["a", "b", "c"])
        self.assertEqual(mix["total_capacity_mw"], 65)

    def test_heat_balance_requires_sample(self):
        result = self.service.heat_balance("b")
        self.assertFalse(result["available"])

    def test_heat_balance_uses_accepted_sample(self):
        self.ingest(1)
        balance = self.service.heat_balance("b")
        self.assertTrue(balance["available"])
        self.assertGreater(balance["thermal_mw"], 0)
        self.assertEqual(balance["delta_c"], 25)

    def test_zone_summary_aggregates_households(self):
        self.ingest(1)
        result = self.service.zone_summary("north")
        self.assertEqual(result["households"], 100)
        self.assertEqual(result["coverage"], 0.5)

    def test_freshness_marks_station_without_samples(self):
        self.ingest(1)
        result = self.service.operations.freshness("2026-11-16T08:10:00+08:00")
        self.assertEqual(result["stale_stations"], ["a", "c"])
        self.assertEqual(result["at_risk_households"], 200)

    def test_freshness_expires_sample(self):
        self.ingest(1)
        result = self.service.operations.freshness("2026-11-16T10:00:00+08:00")
        self.assertIn("b", result["stale_stations"])

    def test_sensor_history_exposes_sequence_gap(self):
        self.ingest(1)
        self.ingest(3)
        result = self.service.operations.sensor_history("sb")
        self.assertEqual(result["sequence_gaps"], [{"from": 2, "to": 2}])

    def test_diagnostics_distinguish_missing_and_rejected(self):
        self.ingest(1)
        item = observation(3)
        self.service.ingestion.ingest("late", [item], "2026-11-16T12:00:00+08:00")
        result = self.service.diagnostics.gaps("sb", 1)
        self.assertEqual(result["missing"], [2])
        self.assertEqual(result["rejected"], [3])

    def test_batch_quality_reasons(self):
        item = observation(1)
        self.service.ingestion.ingest("late", [item], "2026-11-16T12:00:00+08:00")
        result = self.service.diagnostics.batch_quality("2026-11-16T00:00:00Z",
                                                        "2026-11-17T00:00:00Z")
        self.assertEqual(result["rejection_reasons"]["late"], 1)
        self.assertEqual(result["stations"]["b"]["rejected"], 1)

    def test_continuity_detects_gap(self):
        self.ingest(1)
        result = self.service.diagnostics.station_continuity("b",
                   "2026-11-16T08:00:00+08:00", "2026-11-16T09:00:00+08:00", 15)
        self.assertFalse(result["continuous"])
        self.assertGreater(len(result["gaps"]), 0)

    def test_incident_evidence_links_source_ids(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        result = self.service.diagnostics.incident_evidence(1)
        self.assertTrue(result["evidence_complete"])
        self.assertIn("sb-2", result["source_ids"])

    def test_open_risk_deduplicates_households(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        risk = self.service.diagnostics.open_risk_by_zone()
        self.assertEqual(risk["north"]["households"], 300)

    def test_response_queue_prioritizes_incident(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        result = self.service.response.queue("2026-11-16T09:00:00+08:00")
        self.assertEqual(result["unassigned"], [1])
        self.assertEqual(result["incidents"][0]["severity"], "critical")

    def test_response_zones_split_households(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        zones = self.service.response.affected_zone_summary(1)
        self.assertEqual(zones["zones"]["north"]["households"], 100)
        self.assertEqual(zones["zones"]["south"]["households"], 200)

    def test_workload_counts_dispatch(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        self.service.dispatch.assign(1, "crew-a", "2026-11-16T08:12:00+08:00", "operator")
        result = self.service.operations.incident_workload("2026-11-16T00:00:00Z",
                                                           "2026-11-17T00:00:00Z")
        self.assertEqual(result["incident_count"], 1)
        self.assertEqual(result["teams"]["crew-a"]["assigned"], 1)

    def test_audit_is_clean_on_normal_flow(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        self.assertTrue(self.service.operations.audit()["ok"])

    def test_audit_finds_inconsistent_state(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        self.service.store.connection.execute("UPDATE rule_state SET active_incident=NULL")
        result = self.service.operations.audit()
        self.assertFalse(result["ok"])
        self.assertEqual(result["issues"][0]["code"], "orphan_open_incident")

    def test_demo_fixture_runs_end_to_end(self):
        fixture = Path(__file__).parent.parent / "fixtures" / "demo.json"
        result = run(str(fixture))
        self.assertTrue(result["reconciliation"]["ok"])
        self.assertEqual(result["report"]["accepted"], 4)
        self.assertEqual(len(result["report"]["incidents"]), 2)
