import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from heatwatch.api import create_app
from test_service import observation, pipes, rules, stations


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.client = TestClient(create_app(str(Path(self.temporary.name) / "api.sqlite3")))
        response = self.client.put("/network", json={"stations": stations(), "pipes": pipes()})
        self.assertEqual(response.status_code, 200, response.text)
        response = self.client.put("/rules", json={"rules": rules()})
        self.assertEqual(response.status_code, 200, response.text)

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def ingest(self, sequence, pressure=190):
        response = self.client.post("/batches", json={
            "batch_id": f"api-{sequence}", "observations": [observation(sequence, pressure=pressure)],
            "received_at": f"2026-11-16T08:{sequence*5+1:02d}:00+08:00"})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_status_and_network_routes(self):
        status = self.client.get("/status")
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["stations"], 3)
        network = self.client.get("/network")
        self.assertEqual(network.status_code, 200)
        self.assertEqual(len(network.json()["pipes"]), 2)

    def test_incident_route_and_timeline(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        incidents = self.client.get("/incidents", params={"status": "open"})
        self.assertEqual(len(incidents.json()), 1)
        detail = self.client.get("/incidents/1")
        self.assertEqual(detail.json()["incident"]["affected_households"], 300)
        timeline = self.client.get("/stations/b/timeline")
        self.assertEqual(len(timeline.json()["observations"]), 2)

    def test_domain_error_is_structured(self):
        response = self.client.get("/incidents/999")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["code"], "unknown_incident")

    def test_invalid_observation_returns_422(self):
        bad = observation(1)
        bad["return_c"] = 100
        response = self.client.post("/batches", json={
            "batch_id": "bad", "observations": [bad],
            "received_at": "2026-11-16T08:06:00+08:00"})
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["code"], "temperature_order")

    def test_duplicate_batch_route(self):
        item = observation(1)
        payload = {"batch_id": "once", "observations": [item],
                   "received_at": "2026-11-16T08:06:00+08:00"}
        first = self.client.post("/batches", json=payload)
        second = self.client.post("/batches", json=payload)
        self.assertEqual(first.json()["accepted"], 1)
        self.assertEqual(second.json()["status"], "duplicate")

    def test_dispatch_routes_enforce_revision(self):
        self.ingest(1, 170)
        self.ingest(2, 160)
        assigned = self.client.post("/incidents/1/dispatches", json={
            "team": "crew", "at": "2026-11-16T08:12:00+08:00", "actor": "operator"})
        self.assertEqual(assigned.status_code, 200)
        dispatch_id = assigned.json()["id"]
        changed = self.client.patch(f"/dispatches/{dispatch_id}", json={
            "status": "en_route", "expected_revision": 1,
            "at": "2026-11-16T08:13:00+08:00", "actor": "operator"})
        self.assertEqual(changed.json()["revision"], 2)
        conflict = self.client.patch(f"/dispatches/{dispatch_id}", json={
            "status": "on_site", "expected_revision": 1,
            "at": "2026-11-16T08:14:00+08:00", "actor": "operator"})
        self.assertEqual(conflict.status_code, 409)

    def test_report_and_export_routes(self):
        self.ingest(1)
        window = {"from_at": "2026-11-16T00:00:00Z",
                  "to_at": "2026-11-17T00:00:00Z"}
        report = self.client.get("/reports/window", params=window)
        self.assertEqual(report.json()["accepted"], 1)
        result = self.client.post("/exports", json={**window,
            "created_at": "2026-11-16T09:00:00+08:00"})
        self.assertEqual(result.status_code, 200)
        saved = self.client.get(f"/exports/{result.json()['id']}")
        self.assertEqual(saved.json()["digest"], result.json()["digest"])

    def test_capacity_and_response_routes(self):
        demand = {"demands": [{"station": "b", "required_mw": 8, "priority": 5}]}
        plan = self.client.post("/capacity/plan", json=demand)
        self.assertTrue(plan.json()["feasible"])
        simulation = self.client.post("/response/pipes/ab/isolation", json=demand)
        self.assertEqual(simulation.status_code, 200)
        self.assertEqual(simulation.json()["pipe"], "ab")

    def test_quality_and_diagnostics_routes(self):
        self.ingest(1)
        quality = self.client.get("/diagnostics/quality", params={
            "from_at": "2026-11-16T00:00:00Z", "to_at": "2026-11-17T00:00:00Z"})
        self.assertEqual(quality.json()["stations"]["b"]["accepted"], 1)
        gaps = self.client.get("/diagnostics/gaps/sb")
        self.assertEqual(gaps.json()["present"], [1])
        audit = self.client.get("/operations/audit")
        self.assertTrue(audit.json()["ok"])

    def test_replay_is_read_only(self):
        payload = {"batch_id": "unused", "observations": [observation(1)],
                   "received_at": "2026-11-16T08:06:00+08:00"}
        replay = self.client.post("/replay", json=payload)
        self.assertEqual(replay.json()["accepted"], 1)
        self.assertEqual(self.client.get("/status").json()["batches"], 0)
