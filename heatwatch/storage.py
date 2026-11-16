from __future__ import annotations

import contextlib
import json
import sqlite3
from pathlib import Path
from typing import Iterator

from .contracts import DomainError, Observation, Rule
from .topology import Network, Pipe, Station


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS config (
 key TEXT PRIMARY KEY, value TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS stations (
 code TEXT PRIMARY KEY, zone TEXT NOT NULL, households INTEGER NOT NULL,
 source TEXT NOT NULL, capacity_mw REAL NOT NULL, active INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pipes (
 code TEXT PRIMARY KEY, upstream TEXT NOT NULL REFERENCES stations(code),
 downstream TEXT NOT NULL REFERENCES stations(code), max_flow_m3h REAL NOT NULL,
 status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rules (
 name TEXT PRIMARY KEY, metric TEXT NOT NULL, operator TEXT NOT NULL,
 threshold TEXT NOT NULL, severity TEXT NOT NULL, consecutive INTEGER NOT NULL,
 clear_consecutive INTEGER NOT NULL, revision INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS batches (
 batch_id TEXT PRIMARY KEY, digest TEXT NOT NULL, received_at TEXT NOT NULL,
 accepted INTEGER NOT NULL, rejected INTEGER NOT NULL, status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
 id INTEGER PRIMARY KEY AUTOINCREMENT, station TEXT NOT NULL REFERENCES stations(code),
 sensor TEXT NOT NULL, sequence INTEGER NOT NULL, observed_at TEXT NOT NULL,
 supply_c TEXT NOT NULL, return_c TEXT NOT NULL, flow_m3h TEXT NOT NULL,
 pressure_kpa TEXT NOT NULL, gas_m3h TEXT NOT NULL, source_id TEXT NOT NULL UNIQUE,
 batch_id TEXT NOT NULL REFERENCES batches(batch_id), accepted INTEGER NOT NULL,
 reasons TEXT NOT NULL, received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS observation_by_sensor ON observations(sensor,accepted,sequence DESC);
CREATE INDEX IF NOT EXISTS observation_by_station ON observations(station,observed_at);
CREATE TABLE IF NOT EXISTS rule_state (
 station TEXT NOT NULL REFERENCES stations(code), rule TEXT NOT NULL REFERENCES rules(name),
 breach_count INTEGER NOT NULL, clear_count INTEGER NOT NULL,
 active_incident INTEGER, last_observation INTEGER,
 PRIMARY KEY(station,rule)
);
CREATE TABLE IF NOT EXISTS incidents (
 id INTEGER PRIMARY KEY AUTOINCREMENT, station TEXT NOT NULL REFERENCES stations(code),
 rule TEXT NOT NULL REFERENCES rules(name), severity TEXT NOT NULL,
 status TEXT NOT NULL, opened_at TEXT NOT NULL, closed_at TEXT,
 first_observation INTEGER NOT NULL REFERENCES observations(id),
 last_observation INTEGER NOT NULL REFERENCES observations(id),
 affected_households INTEGER NOT NULL, affected_stations TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS incident_open ON incidents(status,station,rule);
CREATE TABLE IF NOT EXISTS incident_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id INTEGER NOT NULL REFERENCES incidents(id),
 event_type TEXT NOT NULL, event_at TEXT NOT NULL, actor TEXT NOT NULL,
 details TEXT NOT NULL, revision INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatches (
 id INTEGER PRIMARY KEY AUTOINCREMENT, incident_id INTEGER NOT NULL REFERENCES incidents(id),
 team TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1,
 UNIQUE(incident_id,team)
);
CREATE TABLE IF NOT EXISTS exports (
 id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
 from_at TEXT NOT NULL, to_at TEXT NOT NULL, payload TEXT NOT NULL,
 digest TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def config(self, key: str, default=None):
        row = self.connection.execute("SELECT value,revision FROM config WHERE key=?", (key,)).fetchone()
        if row is None:
            return default
        return {"value": json.loads(row["value"]), "revision": row["revision"]}

    def set_config(self, key: str, value, expected_revision: int | None = None) -> int:
        current = self.config(key)
        if current is None:
            if expected_revision not in (None, 0):
                raise DomainError("revision_conflict", "configuration revision changed")
            self.connection.execute("INSERT INTO config(key,value) VALUES(?,?)", (key, json.dumps(value, sort_keys=True)))
            return 1
        if expected_revision is not None and expected_revision != current["revision"]:
            raise DomainError("revision_conflict", "configuration revision changed")
        revision = current["revision"] + 1
        self.connection.execute("UPDATE config SET value=?,revision=? WHERE key=?",
                                (json.dumps(value, sort_keys=True), revision, key))
        return revision

    def replace_network(self, network: Network) -> None:
        old_stations = {r[0] for r in self.connection.execute("SELECT code FROM stations")}
        if old_stations and not old_stations.issubset(network.stations):
            missing = sorted(old_stations - set(network.stations))
            raise DomainError("station_removal", "existing station cannot be removed", details={"stations": missing})
        self.connection.execute("DELETE FROM pipes")
        for station in network.stations.values():
            self.connection.execute("""INSERT INTO stations VALUES(?,?,?,?,?,?)
                ON CONFLICT(code) DO UPDATE SET zone=excluded.zone,households=excluded.households,
                source=excluded.source,capacity_mw=excluded.capacity_mw,active=excluded.active""",
                (station.code, station.zone, station.households, station.source,
                 station.capacity_mw, int(station.active)))
        for pipe in network.pipes.values():
            self.connection.execute("INSERT INTO pipes VALUES(?,?,?,?,?)",
                                    (pipe.code, pipe.upstream, pipe.downstream,
                                     pipe.max_flow_m3h, pipe.status))

    def network(self) -> Network:
        stations = [Station(r["code"], r["zone"], r["households"], r["source"],
                            r["capacity_mw"], bool(r["active"]))
                    for r in self.connection.execute("SELECT * FROM stations ORDER BY code")]
        pipes = [Pipe(r["code"], r["upstream"], r["downstream"],
                      r["max_flow_m3h"], r["status"])
                 for r in self.connection.execute("SELECT * FROM pipes ORDER BY code")]
        return Network(stations, pipes)

    def replace_rules(self, rules: list[Rule]) -> None:
        if len({r.name for r in rules}) != len(rules):
            raise DomainError("duplicate_rule", "rule names must be unique")
        old_rules = {r[0] for r in self.connection.execute("SELECT name FROM rules")}
        if old_rules and not old_rules.issubset({r.name for r in rules}):
            raise DomainError("rule_removal", "existing rule cannot be removed")
        for rule in rules:
            self.connection.execute("""INSERT INTO rules(name,metric,operator,threshold,severity,consecutive,clear_consecutive)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET metric=excluded.metric,
                operator=excluded.operator,threshold=excluded.threshold,severity=excluded.severity,
                consecutive=excluded.consecutive,clear_consecutive=excluded.clear_consecutive,
                revision=revision+1""",
                (rule.name, rule.metric, rule.operator, str(rule.threshold),
                 rule.severity, rule.consecutive, rule.clear_consecutive))

    def rules(self) -> list[Rule]:
        return [Rule.parse(dict(r)) for r in self.connection.execute("SELECT * FROM rules ORDER BY name")]

    def batch(self, batch_id: str) -> dict | None:
        row = self.connection.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        return dict(row) if row else None

    def add_batch(self, batch_id: str, digest: str, received_at: str, accepted: int,
                  rejected: int, status: str) -> None:
        self.connection.execute("INSERT INTO batches VALUES(?,?,?,?,?,?)",
                                (batch_id, digest, received_at, accepted, rejected, status))

    def observation_by_source(self, source_id: str) -> dict | None:
        row = self.connection.execute("SELECT * FROM observations WHERE source_id=?", (source_id,)).fetchone()
        return dict(row) if row else None

    def latest_accepted(self, sensor: str) -> Observation | None:
        row = self.connection.execute("""SELECT * FROM observations WHERE sensor=? AND accepted=1
            ORDER BY sequence DESC,id DESC LIMIT 1""", (sensor,)).fetchone()
        return self.decode_observation(row) if row else None

    @staticmethod
    def decode_observation(row) -> Observation:
        return Observation.parse(dict(row))

    def add_observation(self, item: Observation, batch_id: str, accepted: bool,
                        reasons: list[str], received_at: str) -> int:
        cursor = self.connection.execute("""INSERT INTO observations(
            station,sensor,sequence,observed_at,supply_c,return_c,flow_m3h,
            pressure_kpa,gas_m3h,source_id,batch_id,accepted,reasons,received_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (item.station, item.sensor, item.sequence, item.observed_at,
             str(item.supply_c), str(item.return_c), str(item.flow_m3h),
             str(item.pressure_kpa), str(item.gas_m3h), item.source_id,
             batch_id, int(accepted), json.dumps(reasons), received_at))
        return int(cursor.lastrowid)

    def rule_state(self, station: str, rule: str) -> dict:
        row = self.connection.execute("SELECT * FROM rule_state WHERE station=? AND rule=?",
                                      (station, rule)).fetchone()
        return dict(row) if row else {"station": station, "rule": rule,
                                      "breach_count": 0, "clear_count": 0,
                                      "active_incident": None, "last_observation": None}

    def save_rule_state(self, state: dict) -> None:
        self.connection.execute("""INSERT INTO rule_state VALUES(?,?,?,?,?,?)
            ON CONFLICT(station,rule) DO UPDATE SET breach_count=excluded.breach_count,
            clear_count=excluded.clear_count,active_incident=excluded.active_incident,
            last_observation=excluded.last_observation""",
            (state["station"], state["rule"], state["breach_count"], state["clear_count"],
             state["active_incident"], state["last_observation"]))

    def open_incident(self, station: str, rule: Rule, observation_id: int,
                      observed_at: str, exposure: dict) -> int:
        cursor = self.connection.execute("""INSERT INTO incidents(
            station,rule,severity,status,opened_at,first_observation,last_observation,
            affected_households,affected_stations) VALUES(?,?,?,'open',?,?,?,?,?)""",
            (station, rule.name, rule.severity, observed_at, observation_id,
             observation_id, exposure["households"], json.dumps(exposure["stations"])))
        incident_id = int(cursor.lastrowid)
        self.event(incident_id, "opened", observed_at, "system",
                   {"observation_id": observation_id, "exposure": exposure})
        return incident_id

    def incident(self, incident_id: int) -> dict:
        row = self.connection.execute("SELECT * FROM incidents WHERE id=?", (incident_id,)).fetchone()
        if row is None:
            raise DomainError("unknown_incident", "incident does not exist")
        result = dict(row)
        result["affected_stations"] = json.loads(result["affected_stations"])
        return result

    def incidents(self, status: str | None = None) -> list[dict]:
        query = "SELECT id FROM incidents" + (" WHERE status=?" if status else "") + " ORDER BY id"
        return [self.incident(row[0]) for row in self.connection.execute(query, (status,) if status else ())]

    def touch_incident(self, incident_id: int, observation_id: int) -> None:
        self.connection.execute("UPDATE incidents SET last_observation=?,revision=revision+1 WHERE id=?",
                                (observation_id, incident_id))

    def close_incident(self, incident_id: int, observed_at: str, observation_id: int) -> None:
        self.connection.execute("""UPDATE incidents SET status='closed',closed_at=?,
            last_observation=?,revision=revision+1 WHERE id=? AND status='open'""",
            (observed_at, observation_id, incident_id))
        self.event(incident_id, "closed", observed_at, "system", {"observation_id": observation_id})
        pending = self.connection.execute("""SELECT id,team FROM dispatches WHERE incident_id=?
            AND status IN ('assigned','en_route','on_site')""", (incident_id,)).fetchall()
        for row in pending:
            self.connection.execute("""UPDATE dispatches SET status='cancelled',updated_at=?,
                revision=revision+1 WHERE id=?""", (observed_at, row["id"]))
            self.event(incident_id, "team_cancelled", observed_at, "system",
                       {"team": row["team"], "dispatch_id": row["id"],
                        "reason": "incident_closed"})

    def event(self, incident_id: int, kind: str, at: str, actor: str, details: dict) -> int:
        current = self.incident(incident_id)
        cursor = self.connection.execute("""INSERT INTO incident_events(
            incident_id,event_type,event_at,actor,details,revision) VALUES(?,?,?,?,?,?)""",
            (incident_id, kind, at, actor, json.dumps(details, sort_keys=True), current["revision"]))
        return int(cursor.lastrowid)

    def events(self, incident_id: int) -> list[dict]:
        self.incident(incident_id)
        return [{**dict(row), "details": json.loads(row["details"])} for row in
                self.connection.execute("SELECT * FROM incident_events WHERE incident_id=? ORDER BY id",
                                        (incident_id,))]

    def count_observations(self, accepted: bool | None = None) -> int:
        where = "" if accepted is None else " WHERE accepted=?"
        args = () if accepted is None else (int(accepted),)
        return self.connection.execute("SELECT COUNT(*) FROM observations" + where, args).fetchone()[0]

    def observations(self, station: str | None = None, accepted: bool | None = None,
                     from_at: str | None = None, to_at: str | None = None) -> list[dict]:
        clauses, args = [], []
        for column, value in (("station", station), ("accepted", accepted),
                              ("observed_at>=", from_at), ("observed_at<=", to_at)):
            if value is not None:
                clauses.append(column + ("?" if column.endswith((">=", "<=")) else "=?"))
                args.append(int(value) if column == "accepted" else value)
        query = "SELECT * FROM observations" + (" WHERE " + " AND ".join(clauses) if clauses else "") + " ORDER BY observed_at,id"
        return [{**dict(row), "reasons": json.loads(row["reasons"])} for row in self.connection.execute(query, args)]
