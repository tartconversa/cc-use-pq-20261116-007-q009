from __future__ import annotations

from .contracts import DomainError, required_text, utc_time
from .storage import Store


class DispatchService:
    ALLOWED = {"assigned": {"en_route", "cancelled"},
               "en_route": {"on_site", "cancelled"},
               "on_site": {"resolved"}, "resolved": set(), "cancelled": set()}

    def __init__(self, store: Store):
        self.store = store

    def assign(self, incident_id: int, team: str, at: str, actor: str) -> dict:
        team = required_text(team, "team")
        actor = required_text(actor, "actor")
        at = utc_time(at, "at")
        with self.store.transaction():
            incident = self.store.incident(incident_id)
            if incident["status"] != "open":
                raise DomainError("incident_closed", "cannot assign a closed incident")
            row = self.store.connection.execute("SELECT * FROM dispatches WHERE incident_id=? AND team=?",
                                                (incident_id, team)).fetchone()
            if row:
                return dict(row)
            cursor = self.store.connection.execute("""INSERT INTO dispatches(
                incident_id,team,status,created_at,updated_at) VALUES(?,?,'assigned',?,?)""",
                (incident_id, team, at, at))
            self.store.event(incident_id, "team_assigned", at, actor,
                             {"team": team, "dispatch_id": cursor.lastrowid})
            return dict(self.store.connection.execute("SELECT * FROM dispatches WHERE id=?",
                                                      (cursor.lastrowid,)).fetchone())

    def transition(self, dispatch_id: int, status: str, expected_revision: int,
                   at: str, actor: str) -> dict:
        at = utc_time(at, "at")
        actor = required_text(actor, "actor")
        with self.store.transaction():
            row = self.store.connection.execute("SELECT * FROM dispatches WHERE id=?",
                                                (dispatch_id,)).fetchone()
            if row is None:
                raise DomainError("unknown_dispatch", "dispatch does not exist")
            current = dict(row)
            if current["revision"] != expected_revision:
                raise DomainError("revision_conflict", "dispatch revision changed")
            if status not in self.ALLOWED[current["status"]]:
                raise DomainError("invalid_transition", "dispatch status transition is invalid")
            if status != "cancelled" and self.store.incident(current["incident_id"])["status"] != "open":
                raise DomainError("incident_closed", "incident is already closed")
            self.store.connection.execute("""UPDATE dispatches SET status=?,updated_at=?,
                revision=revision+1 WHERE id=?""", (status, at, dispatch_id))
            self.store.event(current["incident_id"], "team_" + status, at, actor,
                             {"team": current["team"], "dispatch_id": dispatch_id})
            return dict(self.store.connection.execute("SELECT * FROM dispatches WHERE id=?",
                                                      (dispatch_id,)).fetchone())

    def for_incident(self, incident_id: int) -> list[dict]:
        self.store.incident(incident_id)
        return [dict(row) for row in self.store.connection.execute(
            "SELECT * FROM dispatches WHERE incident_id=? ORDER BY id", (incident_id,))]
