import json
import tempfile
import unittest
from pathlib import Path

from heatwatch.contracts import DomainError
from heatwatch.service import HeatService


BASE = "2026-11-16T08:00:00+08:00"


def observation(sequence, pressure=190, supply=75, station="b", sensor="sb", suffix=None):
    minute = sequence * 5
    return {"station": station, "sensor": sensor, "sequence": sequence,
            "observed_at": f"2026-11-16T08:{minute:02d}:00+08:00",
            "supply_c": supply, "return_c": 50,
            "flow_m3h": 20, "pressure_kpa": pressure,
            "gas_m3h": 3, "source_id": suffix or f"{sensor}-{sequence}"}


def stations():
    return [{"code": "a", "zone": "north", "households": 0,
             "source": "waste_heat", "capacity_mw": 50},
            {"code": "b", "zone": "north", "households": 100,
             "source": "gas", "capacity_mw": 10},
            {"code": "c", "zone": "south", "households": 200,
             "source": "storage", "capacity_mw": 5}]


def pipes():
    return [{"code": "ab", "upstream": "a", "downstream": "b", "max_flow_m3h": 50},
            {"code": "bc", "upstream": "b", "downstream": "c", "max_flow_m3h": 20}]


def rules():
    return [{"name": "low_pressure", "metric": "pressure_kpa", "operator": "lt",
             "threshold": 180, "severity": "critical", "consecutive": 2,
             "clear_consecutive": 2}]


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temporary.name) / "db.sqlite3")
        self.service = HeatService(self.path)
        self.service.configure_network(stations(), pipes())
        self.service.configure_rules(rules())

    def tearDown(self):
        self.service.close()
        self.temporary.cleanup()

    def ingest(self, sequence, **changes):
        item = observation(sequence, **changes)
        return self.service.ingestion.ingest(f"batch-{sequence}", [item],
                                             f"2026-11-16T08:{sequence*5+1:02d}:00+08:00")

    def test_network_and_rules_configured(self):
        status = self.service.status()
        self.assertEqual(status["stations"], 3)
        self.assertEqual(status["pipes"], 2)
        self.assertEqual(status["rules"], 1)
        self.assertTrue(status["reconciliation"]["ok"])

    def test_two_breaches_open_one_incident(self):
        first = self.ingest(1, pressure=170)
        second = self.ingest(2, pressure=160)
        self.assertEqual(first["transitions"], [])
        self.assertEqual(len(second["transitions"]), 1)
        incident = self.service.store.incidents("open")[0]
        self.assertEqual(incident["affected_households"], 300)
        self.assertEqual(incident["affected_stations"], ["b", "c"])
        self.assertEqual(self.service.store.events(incident["id"])[0]["event_type"], "opened")

    def test_two_clear_samples_close_incident(self):
        self.ingest(1, pressure=170)
        self.ingest(2, pressure=160)
        self.assertEqual(self.ingest(3, pressure=190)["transitions"], [])
        result = self.ingest(4, pressure=190)
        self.assertEqual(result["transitions"][0]["type"], "closed")
        self.assertEqual(len(self.service.store.incidents("open")), 0)
        self.assertEqual(len(self.service.store.events(1)), 2)

    def test_duplicate_batch_is_idempotent(self):
        item = observation(1)
        received = "2026-11-16T08:06:00+08:00"
        first = self.service.ingestion.ingest("once", [item], received)
        second = self.service.ingestion.ingest("once", [item], received)
        self.assertEqual(first["accepted"], 1)
        self.assertEqual(second["status"], "duplicate")
        self.assertEqual(self.service.store.count_observations(), 1)

    def test_changed_batch_id_conflicts(self):
        item = observation(1)
        self.service.ingestion.ingest("once", [item], "2026-11-16T08:06:00+08:00")
        item2 = observation(2)
        with self.assertRaises(DomainError) as caught:
            self.service.ingestion.ingest("once", [item2], "2026-11-16T08:11:00+08:00")
        self.assertEqual(caught.exception.code, "batch_conflict")

    def test_duplicate_source_rolls_back_entire_batch(self):
        self.ingest(1)
        item = observation(2, suffix="sb-1")
        with self.assertRaises(DomainError):
            self.service.ingestion.ingest("bad", [item], "2026-11-16T08:11:00+08:00")
        self.assertIsNone(self.service.store.batch("bad"))
        self.assertEqual(self.service.store.count_observations(), 1)

    def test_late_sample_retained_but_no_alert(self):
        item = observation(1, pressure=150)
        result = self.service.ingestion.ingest("late", [item],
                                               "2026-11-16T12:00:00+08:00")
        self.assertEqual(result["rejected"], 1)
        self.assertFalse(self.service.store.incidents())
        self.assertIn("late", self.service.store.observations()[0]["reasons"])

    def test_future_sample_rejected(self):
        result = self.service.ingestion.ingest("future", [observation(1)],
                                               "2026-11-16T08:00:00+08:00")
        self.assertEqual(result["rejected"], 0)
        item = observation(2)
        result = self.service.ingestion.ingest("future-2", [item],
                                               "2026-11-16T08:01:00+08:00")
        self.assertEqual(result["rejected"], 1)

    def test_out_of_order_sequence_is_rejected(self):
        self.ingest(2)
        item = observation(1)
        result = self.service.ingestion.ingest("older", [item],
                                               "2026-11-16T08:11:00+08:00")
        self.assertEqual(result["rejected"], 1)
        rejected = self.service.store.observation_by_source("sb-1")
        self.assertIn("out_of_order_sequence", json.loads(rejected["reasons"]))

    def test_rejected_sample_does_not_break_streak(self):
        self.ingest(1, pressure=170)
        bad = observation(2, pressure=160)
        self.service.ingestion.ingest("late", [bad], "2026-11-16T12:00:00+08:00")
        result = self.ingest(3, pressure=150)
        self.assertEqual(result["transitions"][0]["type"], "opened")

    def test_sensor_cannot_move_between_stations(self):
        self.ingest(1)
        moved = observation(2, station="c")
        with self.assertRaises(DomainError) as caught:
            self.service.ingestion.ingest("move", [moved], "2026-11-16T08:11:00+08:00")
        self.assertEqual(caught.exception.code, "sensor_moved")

    def test_rule_state_tracks_last_observation(self):
        self.ingest(1, pressure=170)
        state = self.service.store.rule_state("b", "low_pressure")
        self.assertEqual(state["breach_count"], 1)
        self.assertIsNotNone(state["last_observation"])

    def test_report_distinguishes_accepted_and_rejected(self):
        self.ingest(1)
        item = observation(2)
        self.service.ingestion.ingest("late", [item], "2026-11-16T12:00:00+08:00")
        report = self.service.reporting.window("2026-11-16T00:00:00Z",
                                                "2026-11-17T00:00:00Z")
        self.assertEqual(report["accepted"], 1)
        self.assertEqual(report["rejected"], 1)
        self.assertEqual(report["stations"][0]["average_supply_c"], 75)

    def test_reconcile_counts(self):
        self.ingest(1)
        result = self.service.reporting.reconcile()
        self.assertTrue(result["ok"])
        self.assertEqual(result["counts"]["accepted"], 1)

    def test_export_digest_survives_reopen(self):
        self.ingest(1)
        exported = self.service.reporting.export("2026-11-16T00:00:00Z",
                                                 "2026-11-17T00:00:00Z", BASE)
        self.assertEqual(len(exported["digest"]), 64)
        self.service.close()
        self.service = HeatService(self.path)
        saved = self.service.reporting.saved_export(exported["id"])
        self.assertEqual(saved["digest"], exported["digest"])
        self.assertEqual(saved["report"]["accepted"], 1)

    def test_export_detects_corruption(self):
        exported = self.service.reporting.export(BASE, "2026-11-17T00:00:00Z", BASE)
        self.service.store.connection.execute("UPDATE exports SET payload='{}' WHERE id=?",
                                              (exported["id"],))
        with self.assertRaises(DomainError) as caught:
            self.service.reporting.saved_export(exported["id"])
        self.assertEqual(caught.exception.code, "export_corrupt")

    def test_dispatch_lifecycle_and_revision(self):
        self.ingest(1, pressure=170)
        self.ingest(2, pressure=160)
        assigned = self.service.dispatch.assign(1, "team-1", BASE, "operator")
        self.assertEqual(assigned["status"], "assigned")
        repeated = self.service.dispatch.assign(1, "team-1", BASE, "operator")
        self.assertEqual(repeated["id"], assigned["id"])
        en_route = self.service.dispatch.transition(assigned["id"], "en_route", 1,
                                                    BASE, "operator")
        self.assertEqual(en_route["revision"], 2)
        with self.assertRaises(DomainError) as caught:
            self.service.dispatch.transition(assigned["id"], "on_site", 1,
                                             BASE, "operator")
        self.assertEqual(caught.exception.code, "revision_conflict")

    def test_dispatch_transition_order_is_enforced(self):
        self.ingest(1, pressure=170)
        self.ingest(2, pressure=160)
        assigned = self.service.dispatch.assign(1, "team-1", BASE, "operator")
        with self.assertRaises(DomainError) as caught:
            self.service.dispatch.transition(assigned["id"], "resolved", 1,
                                             BASE, "operator")
        self.assertEqual(caught.exception.code, "invalid_transition")

    def test_network_revision_conflict(self):
        with self.assertRaises(DomainError) as caught:
            self.service.configure_network(stations(), pipes(), expected_revision=0)
        self.assertEqual(caught.exception.code, "revision_conflict")

    def test_topology_change_reprojects_open_incident(self):
        self.ingest(1, pressure=170)
        self.ingest(2, pressure=160)
        self.assertEqual(self.service.store.incident(1)["affected_households"], 300)
        updated = [{**pipe, "status": "closed"} if pipe["code"] == "bc" else pipe
                   for pipe in pipes()]
        result = self.service.configure_network(stations(), updated, expected_revision=1)
        self.assertEqual(result["reprojected_incidents"], 1)
        self.assertEqual(self.service.store.incident(1)["affected_households"], 100)
        self.assertEqual(self.service.store.events(1)[-1]["event_type"], "exposure_changed")

    def test_replay_does_not_persist(self):
        result = self.service.ingestion.replay([observation(1)],
                                                "2026-11-16T08:06:00+08:00")
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(self.service.store.count_observations(), 0)

    def test_timeline_links_records_and_incidents(self):
        self.ingest(1, pressure=170)
        self.ingest(2, pressure=160)
        timeline = self.service.timeline("b")
        self.assertEqual(len(timeline["observations"]), 2)
        self.assertEqual(len(timeline["incidents"]), 1)
        self.assertEqual(timeline["exposure"]["households"], 300)
