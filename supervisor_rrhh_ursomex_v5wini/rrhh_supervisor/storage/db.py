from __future__ import annotations

import json
import os
import sqlite3
import re
import threading
from urllib.parse import quote
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class DB:
    def __init__(self, engine: str, sqlite_path: str, postgres_dsn: str = "", init_rrhh_schema: bool = True, sqlite_read_only: bool = False):
        self.engine = (engine or "sqlite").lower().strip()
        if self.engine not in ("sqlite", "postgres"):
            raise ValueError("database.engine debe ser sqlite o postgres")
        self.sqlite_path = sqlite_path
        self.postgres_dsn = postgres_dsn
        self.init_rrhh_schema = bool(init_rrhh_schema)
        self.sqlite_read_only = bool(sqlite_read_only)
        self._conn = None
        self._lock = threading.RLock()
        if self.engine == "sqlite":
            if self.sqlite_read_only:
                # Construye URI robusto (Windows/espacios) para modo solo-lectura.
                p = str(self.sqlite_path).replace('\\', '/')
                if re.match(r'^[A-Za-z]:/', p):
                    p = '/' + p
                p = quote(p, safe='/:')
                uri = f"file:{p}?mode=ro"
                self._conn = sqlite3.connect(uri, uri=True, check_same_thread=False, timeout=30)
            else:
                Path(os.path.dirname(self.sqlite_path)).mkdir(parents=True, exist_ok=True)
                self._conn = sqlite3.connect(self.sqlite_path, check_same_thread=False, timeout=30)
                self._conn.execute("PRAGMA journal_mode=WAL;")
                self._conn.execute("PRAGMA foreign_keys=ON;")
                if self.init_rrhh_schema:
                    self._ensure_rrhh_tables_sqlite()
        else:
            import psycopg
            self._conn = psycopg.connect(self.postgres_dsn, autocommit=True)
            if self.init_rrhh_schema:
                self._ensure_rrhh_tables_postgres()
    def close(self):
        if self._conn is None:
            return
        with self._lock:
            self._conn.close()
            self._conn = None

    def _ensure_rrhh_tables_sqlite(self):
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rrhh_state (
                  k TEXT PRIMARY KEY,
                  v TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rrhh_employee_profile (
                  employee_id TEXT NOT NULL,
                  window_days INTEGER NOT NULL,
                  updated_at TEXT NOT NULL,
                  profile_json TEXT NOT NULL,
                  PRIMARY KEY (employee_id, window_days)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rrhh_roster (
                  employee_id TEXT PRIMARY KEY,
                  employee_name TEXT,
                  active INTEGER NOT NULL DEFAULT 1,
                  source TEXT NOT NULL DEFAULT '',
                  updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_rrhh_roster_active ON rrhh_roster(active)")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rrhh_permissions (
                  employee_id TEXT NOT NULL,
                  op_date TEXT NOT NULL,          -- YYYY-MM-DD (día operativo)
                  reason TEXT NOT NULL DEFAULT '',
                  source TEXT NOT NULL DEFAULT '',
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (employee_id, op_date)
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_rrhh_permissions_opdate ON rrhh_permissions(op_date)")
            self._conn.commit()

    def _ensure_rrhh_tables_postgres(self):
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rrhh_state (
              k TEXT PRIMARY KEY,
              v TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rrhh_employee_profile (
              employee_id TEXT NOT NULL,
              window_days INTEGER NOT NULL,
              updated_at TEXT NOT NULL,
              profile_json TEXT NOT NULL,
              PRIMARY KEY (employee_id, window_days)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rrhh_roster (
              employee_id TEXT PRIMARY KEY,
              employee_name TEXT,
              active SMALLINT NOT NULL DEFAULT 1,
              source TEXT NOT NULL DEFAULT '',
              updated_at TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rrhh_roster_active ON rrhh_roster(active)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rrhh_permissions (
              employee_id TEXT NOT NULL,
              op_date TEXT NOT NULL,
              reason TEXT NOT NULL DEFAULT '',
              source TEXT NOT NULL DEFAULT '',
              updated_at TEXT NOT NULL,
              PRIMARY KEY (employee_id, op_date)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rrhh_permissions_opdate ON rrhh_permissions(op_date)")


    def get_state(self, k: str) -> str:
        k = (k or "").strip()
        if not k:
            return ""
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute("SELECT v FROM rrhh_state WHERE k = ?", (k,)).fetchone()
            return str(row[0]) if row else ""
        cur = self._conn.cursor()
        cur.execute("SELECT v FROM rrhh_state WHERE k=%s", (k,))
        row = cur.fetchone()
        return str(row[0]) if row else ""

    def upsert_state(self, k: str, v: str):
        k = (k or "").strip()
        if not k:
            return
        v = "" if v is None else str(v)
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO rrhh_state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                    (k, v),
                )
                self._conn.commit()
            return
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO rrhh_state(k,v) VALUES(%s,%s)
            ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v
            """,
            (k, v),
        )
    def upsert_roster(self, records: List[Dict[str, Any]], source: str = "csv"):
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        rows = []
        for r in (records or []):
            emp = str(r.get("employee_id") or "").strip()
            if not emp:
                continue
            name = str(r.get("employee_name") or "").strip()
            active = 1 if int(r.get("active", 1) or 0) else 0
            rows.append((emp, name, active, str(source or ""), now))
        if not rows:
            return
        if self.engine == "sqlite":
            with self._lock:
                self._conn.executemany(
                    """
                    INSERT INTO rrhh_roster(employee_id, employee_name, active, source, updated_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(employee_id) DO UPDATE SET
                      employee_name=excluded.employee_name,
                      active=excluded.active,
                      source=excluded.source,
                      updated_at=excluded.updated_at
                    """,
                    rows,
                )
                self._conn.commit()
            return
        cur = self._conn.cursor()
        for emp, name, active, src, upd in rows:
            cur.execute(
                """
                INSERT INTO rrhh_roster(employee_id, employee_name, active, source, updated_at)
                VALUES(%s,%s,%s,%s,%s)
                ON CONFLICT (employee_id) DO UPDATE SET
                  employee_name=EXCLUDED.employee_name,
                  active=EXCLUDED.active,
                  source=EXCLUDED.source,
                  updated_at=EXCLUDED.updated_at
                """,
                (emp, name, int(active), src, upd),
            )

    def list_roster(self, active_only: bool = True, limit: int = 0) -> List[Dict[str, Any]]:
        where = "WHERE active = 1" if active_only else ""
        lim = ""
        if int(limit or 0) > 0:
            lim = "LIMIT " + str(int(limit))
        q = f"SELECT employee_id, COALESCE(employee_name,''), active, COALESCE(source,''), updated_at FROM rrhh_roster {where} ORDER BY employee_id {lim}"
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(q).fetchall()
            return [
                {"employee_id": str(r[0]), "employee_name": str(r[1] or ""), "active": int(r[2] or 0), "source": str(r[3] or ""), "updated_at": str(r[4] or "")}
                for r in rows
            ]
        cur = self._conn.cursor()
        cur.execute(q)
        rows = cur.fetchall()
        return [
            {"employee_id": str(r[0]), "employee_name": str(r[1] or ""), "active": int(r[2] or 0), "source": str(r[3] or ""), "updated_at": str(r[4] or "")}
            for r in rows
        ]



    def upsert_permissions(self, rows: List[Dict[str, Any]], source: str = "csv") -> None:
        """Inserta/actualiza permisos (día no laborable justificado) por empleado y op_date."""
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        src = str(source or "csv")
        items = []
        for r in (rows or []):
            eid = str(r.get("employee_id") or r.get("id") or "").strip()
            od = str(r.get("op_date") or r.get("date") or r.get("day") or "").strip()
            if not eid or not od:
                continue
            reason = str(r.get("reason") or r.get("motivo") or "").strip()
            items.append((eid, od, reason, src, now))
        if not items:
            return

        if self.engine == "sqlite":
            with self._lock:
                self._conn.executemany(
                    """
                    INSERT INTO rrhh_permissions(employee_id, op_date, reason, source, updated_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(employee_id, op_date) DO UPDATE SET
                      reason=excluded.reason,
                      source=excluded.source,
                      updated_at=excluded.updated_at
                    """,
                    items,
                )
                self._conn.commit()
            return

        cur = self._conn.cursor()
        cur.executemany(
            """
            INSERT INTO rrhh_permissions(employee_id, op_date, reason, source, updated_at)
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT(employee_id, op_date) DO UPDATE SET
              reason=EXCLUDED.reason,
              source=EXCLUDED.source,
              updated_at=EXCLUDED.updated_at
            """,
            items,
        )
        self._conn.commit()

    def list_permissions_opdate_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        s = str(start_date or "").strip()
        e = str(end_date or "").strip()
        if not s or not e:
            return []
        q = (
            "SELECT employee_id, op_date, COALESCE(reason,''), COALESCE(source,''), updated_at "
            "FROM rrhh_permissions WHERE op_date >= ? AND op_date <= ? ORDER BY employee_id, op_date"
        )
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(q, (s, e)).fetchall()
            return [
                {"employee_id": str(r[0]), "op_date": str(r[1]), "reason": str(r[2] or ""), "source": str(r[3] or ""), "updated_at": str(r[4] or "")}
                for r in rows
            ]

        q2 = (
            "SELECT employee_id, op_date, COALESCE(reason,''), COALESCE(source,''), updated_at "
            "FROM rrhh_permissions WHERE op_date >= %s AND op_date <= %s ORDER BY employee_id, op_date"
        )
        cur = self._conn.cursor()
        cur.execute(q2, (s, e))
        rows = cur.fetchall()
        return [
            {"employee_id": str(r[0]), "op_date": str(r[1]), "reason": str(r[2] or ""), "source": str(r[3] or ""), "updated_at": str(r[4] or "")}
            for r in rows
        ]

    def permissions_set_opdate_range(self, start_date: str, end_date: str) -> Dict[str, set]:
        """Conveniencia: regresa dict employee_id -> set(op_date)."""
        out: Dict[str, set] = {}
        for r in self.list_permissions_opdate_range(start_date, end_date):
            out.setdefault(str(r["employee_id"]), set()).add(str(r["op_date"]))
        return out

    def search_employees_any(self, q: str, limit: int = 25) -> List[Dict[str, Any]]:
        s = (q or "").strip()
        if not s:
            return []
        like = f"%{s}%"
        out: Dict[str, Dict[str, Any]] = {}
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    "SELECT employee_id, COALESCE(employee_name,'') FROM rrhh_roster WHERE employee_id LIKE ? OR employee_name LIKE ? ORDER BY employee_id LIMIT ?",
                    (like, like, int(limit)),
                ).fetchall()
            for r in rows:
                out[str(r[0])] = {"employee_id": str(r[0]), "employee_name": str(r[1] or "")}
        else:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT employee_id, COALESCE(employee_name,'') FROM rrhh_roster WHERE employee_id ILIKE %s OR employee_name ILIKE %s ORDER BY employee_id LIMIT %s",
                (like, like, int(limit)),
            )
            for r in cur.fetchall():
                out[str(r[0])] = {"employee_id": str(r[0]), "employee_name": str(r[1] or "")}
        if len(out) < int(limit):
            extra = self.search_employees(s, limit=int(limit))
            for r in extra:
                if r.get("employee_id") and r["employee_id"] not in out:
                    out[r["employee_id"]] = r
        return list(out.values())[: int(limit)]

    def list_jornadas_closed_opdate_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        sd = (start_date or "").strip()
        ed = (end_date or "").strip()
        if not sd or not ed:
            return []
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    """
                    SELECT employee_id, COALESCE(employee_name,''), op_date, start_time, start_time_utc,
                           end_time, end_time_utc, duration_minutes, events_count, incidencias_json
                    FROM jornadas
                    WHERE closed = 1
                      AND op_date >= ?
                      AND op_date <= ?
                    ORDER BY op_date, employee_id, start_time_utc
                    """,
                    (sd, ed),
                ).fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "employee_id": str(r[0]),
                        "employee_name": str(r[1] or ""),
                        "op_date": str(r[2]),
                        "start_time": str(r[3]),
                        "start_time_utc": str(r[4]),
                        "end_time": str(r[5] or ""),
                        "end_time_utc": str(r[6] or ""),
                        "duration_minutes": int(r[7] or 0),
                        "events_count": int(r[8] or 0),
                        "incidencias": json.loads(r[9] or "[]") if r[9] else [],
                    }
                )
            return out
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT employee_id, COALESCE(employee_name,''), op_date, start_time, start_time_utc,
                   end_time, end_time_utc, duration_minutes, events_count, incidencias_json
            FROM jornadas
            WHERE closed = 1
              AND op_date >= %s
              AND op_date <= %s
            ORDER BY op_date, employee_id, start_time_utc
            """,
            (sd, ed),
        )
        rows = cur.fetchall()
        out = []
        for r in rows:
            incid = r[9] if r[9] else []
            try:
                if isinstance(incid, str):
                    incid = json.loads(incid)
            except Exception:
                incid = []
            out.append(
                {
                    "employee_id": str(r[0]),
                    "employee_name": str(r[1] or ""),
                    "op_date": str(r[2]),
                    "start_time": str(r[3]),
                    "start_time_utc": str(r[4]),
                    "end_time": str(r[5] or ""),
                    "end_time_utc": str(r[6] or ""),
                    "duration_minutes": int(r[7] or 0),
                    "events_count": int(r[8] or 0),
                    "incidencias": incid,
                }
            )
        return out

    def list_employee_ids_with_opdate(self, op_date: str) -> List[str]:
        od = (op_date or "").strip()
        if not od:
            return []
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    "SELECT DISTINCT employee_id FROM jornadas WHERE op_date = ?",
                    (od,),
                ).fetchall()
            return [str(r[0]) for r in rows if r and r[0] is not None]
        cur = self._conn.cursor()
        cur.execute("SELECT DISTINCT employee_id FROM jornadas WHERE op_date = %s", (od,))
        rows = cur.fetchall()
        return [str(r[0]) for r in rows if r and r[0] is not None]

    def get_latest_event_for_employee(self, employee_id: str) -> Optional[Dict[str, Any]]:
        emp = (employee_id or "").strip()
        if not emp:
            return None
        if self.engine == "sqlite":
            with self._lock:
                r = self._conn.execute(
                    """
                    SELECT je.employee_id, je.role, je.event_time_utc, je.event_time, j.employee_name, je.jornada_id
                    FROM jornada_events je
                    LEFT JOIN jornadas j ON j.jornada_id = je.jornada_id
                    WHERE je.employee_id = ?
                    ORDER BY je.event_time_utc DESC
                    LIMIT 1
                    """,
                    (emp,),
                ).fetchone()
            if not r:
                return None
            return {
                "employee_id": str(r[0]),
                "role": str(r[1]),
                "event_time_utc": str(r[2]),
                "event_time": str(r[3]),
                "employee_name": str(r[4] or ""),
                "jornada_id": str(r[5] or ""),
            }
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT je.employee_id, je.role, je.event_time_utc, je.event_time, j.employee_name, je.jornada_id
            FROM jornada_events je
            LEFT JOIN jornadas j ON j.jornada_id = je.jornada_id
            WHERE je.employee_id = %s
            ORDER BY je.event_time_utc DESC
            LIMIT 1
            """,
            (emp,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "employee_id": str(r[0]),
            "role": str(r[1]),
            "event_time_utc": str(r[2]),
            "event_time": str(r[3]),
            "employee_name": str(r[4] or ""),
            "jornada_id": str(r[5] or ""),
        }

    def get_open_jornada_for_employee(self, employee_id: str) -> Optional[Dict[str, Any]]:
        emp = (employee_id or "").strip()
        if not emp:
            return None
        if self.engine == "sqlite":
            with self._lock:
                r = self._conn.execute(
                    """
                    SELECT employee_id, jornada_id, op_date, start_time, start_time_utc,
                           end_time, end_time_utc, events_count, updated_at, COALESCE(employee_name,''), closed
                    FROM jornadas
                    WHERE employee_id = ? AND closed = 0
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (emp,),
                ).fetchone()
            if not r:
                return None
            return {
                "employee_id": str(r[0]),
                "jornada_id": str(r[1]),
                "op_date": str(r[2]),
                "start_time": str(r[3]),
                "start_time_utc": str(r[4]),
                "end_time": str(r[5] or ""),
                "end_time_utc": str(r[6] or ""),
                "events_count": int(r[7] or 0),
                "updated_at": str(r[8]),
                "employee_name": str(r[9] or ""),
                "closed": int(r[10] or 0),
            }
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT employee_id, jornada_id, op_date, start_time, start_time_utc,
                   end_time, end_time_utc, events_count, updated_at, COALESCE(employee_name,''), closed
            FROM jornadas
            WHERE employee_id = %s AND closed = 0
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (emp,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "employee_id": str(r[0]),
            "jornada_id": str(r[1]),
            "op_date": str(r[2]),
            "start_time": str(r[3]),
            "start_time_utc": str(r[4]),
            "end_time": str(r[5] or ""),
            "end_time_utc": str(r[6] or ""),
            "events_count": int(r[7] or 0),
            "updated_at": str(r[8]),
            "employee_name": str(r[9] or ""),
            "closed": int(r[10] or 0),
        }



    def list_active_employees_last_days(self, since_utc_iso: str) -> List[Dict[str, Any]]:
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    """
                    SELECT employee_id, MAX(COALESCE(employee_name,'')) AS employee_name
                    FROM jornadas
                    WHERE start_time_utc >= ?
                    GROUP BY employee_id
                    ORDER BY employee_id
                    """,
                    (since_utc_iso,),
                ).fetchall()
            return [{"employee_id": str(r[0]), "employee_name": str(r[1] or "")} for r in rows if r and r[0]]
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT employee_id, MAX(COALESCE(employee_name,'')) AS employee_name
            FROM jornadas
            WHERE start_time_utc >= %s
            GROUP BY employee_id
            ORDER BY employee_id
            """,
            (since_utc_iso,),
        )
        rows = cur.fetchall()
        return [{"employee_id": str(r[0]), "employee_name": str(r[1] or "")} for r in rows if r and r[0]]

    def get_latest_event_per_employee(self) -> List[Dict[str, Any]]:
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    """
                    WITH last AS (
                      SELECT employee_id, MAX(event_time_utc) AS max_utc
                      FROM jornada_events
                      GROUP BY employee_id
                    )
                    SELECT je.employee_id, je.role, je.event_time_utc, je.event_time, j.employee_name
                    FROM jornada_events je
                    JOIN last ON last.employee_id = je.employee_id AND last.max_utc = je.event_time_utc
                    LEFT JOIN jornadas j ON j.jornada_id = je.jornada_id
                    """,
                ).fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "employee_id": str(r[0]),
                        "role": str(r[1]),
                        "event_time_utc": str(r[2]),
                        "event_time": str(r[3]),
                        "employee_name": str(r[4] or ""),
                    }
                )
            return out
        cur = self._conn.cursor()
        cur.execute(
            """
            WITH last AS (
              SELECT employee_id, MAX(event_time_utc) AS max_utc
              FROM jornada_events
              GROUP BY employee_id
            )
            SELECT je.employee_id, je.role, je.event_time_utc, je.event_time, j.employee_name
            FROM jornada_events je
            JOIN last ON last.employee_id = je.employee_id AND last.max_utc = je.event_time_utc
            LEFT JOIN jornadas j ON j.jornada_id = je.jornada_id
            """
        )
        rows = cur.fetchall()
        return [
            {
                "employee_id": str(r[0]),
                "role": str(r[1]),
                "event_time_utc": str(r[2]),
                "event_time": str(r[3]),
                "employee_name": str(r[4] or ""),
            }
            for r in rows
        ]

    def get_open_jornadas(self) -> List[Dict[str, Any]]:
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    """
                    WITH last AS (
                      SELECT employee_id, MAX(updated_at) AS mu
                      FROM jornadas
                      WHERE closed = 0
                      GROUP BY employee_id
                    )
                    SELECT j.employee_id, j.jornada_id, j.op_date, j.start_time, j.start_time_utc,
                           j.end_time, j.end_time_utc, j.events_count, j.updated_at, COALESCE(j.employee_name,'')
                    FROM jornadas j
                    JOIN last ON last.employee_id = j.employee_id AND last.mu = j.updated_at
                    WHERE j.closed = 0
                    """,
                ).fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "employee_id": str(r[0]),
                        "jornada_id": str(r[1]),
                        "op_date": str(r[2]),
                        "start_time": str(r[3]),
                        "start_time_utc": str(r[4]),
                        "end_time": str(r[5] or ""),
                        "end_time_utc": str(r[6] or ""),
                        "events_count": int(r[7] or 0),
                        "updated_at": str(r[8]),
                        "employee_name": str(r[9] or ""),
                    }
                )
            return out
        cur = self._conn.cursor()
        cur.execute(
            """
            WITH last AS (
              SELECT employee_id, MAX(updated_at) AS mu
              FROM jornadas
              WHERE closed = 0
              GROUP BY employee_id
            )
            SELECT j.employee_id, j.jornada_id, j.op_date, j.start_time, j.start_time_utc,
                   j.end_time, j.end_time_utc, j.events_count, j.updated_at, COALESCE(j.employee_name,'')
            FROM jornadas j
            JOIN last ON last.employee_id = j.employee_id AND last.mu = j.updated_at
            WHERE j.closed = 0
            """
        )
        rows = cur.fetchall()
        return [
            {
                "employee_id": str(r[0]),
                "jornada_id": str(r[1]),
                "op_date": str(r[2]),
                "start_time": str(r[3]),
                "start_time_utc": str(r[4]),
                "end_time": str(r[5] or ""),
                "end_time_utc": str(r[6] or ""),
                "events_count": int(r[7] or 0),
                "updated_at": str(r[8]),
                "employee_name": str(r[9] or ""),
            }
            for r in rows
        ]


    def search_roster(self, q: str, limit: int = 25) -> List[Dict[str, Any]]:
        s = (q or "").strip()
        if not s:
            return []
        like = f"%{s}%"
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    "SELECT employee_id, COALESCE(employee_name,'') FROM rrhh_roster WHERE employee_id LIKE ? OR employee_name LIKE ? ORDER BY employee_id LIMIT ?",
                    (like, like, int(limit)),
                ).fetchall()
            return [{"employee_id": str(r[0]), "employee_name": str(r[1] or "")} for r in rows]
        cur = self._conn.cursor()
        cur.execute(
            "SELECT employee_id, COALESCE(employee_name,'') FROM rrhh_roster WHERE employee_id ILIKE %s OR employee_name ILIKE %s ORDER BY employee_id LIMIT %s",
            (like, like, int(limit)),
        )
        return [{"employee_id": str(r[0]), "employee_name": str(r[1] or "")} for r in cur.fetchall()]

    def search_employees(self, q: str, limit: int = 25) -> List[Dict[str, Any]]:
        s = (q or "").strip()
        if not s:
            return []
        like = f"%{s}%"
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    """
                    SELECT employee_id, MAX(COALESCE(employee_name,'')) AS employee_name
                    FROM jornadas
                    WHERE employee_id LIKE ? OR employee_name LIKE ?
                    GROUP BY employee_id
                    ORDER BY employee_id
                    LIMIT ?
                    """,
                    (like, like, int(limit)),
                ).fetchall()
            return [{"employee_id": str(r[0]), "employee_name": str(r[1] or "")} for r in rows if r and r[0]]
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT employee_id, MAX(COALESCE(employee_name,'')) AS employee_name
            FROM jornadas
            WHERE employee_id ILIKE %s OR employee_name ILIKE %s
            GROUP BY employee_id
            ORDER BY employee_id
            LIMIT %s
            """,
            (like, like, int(limit)),
        )
        rows = cur.fetchall()
        return [{"employee_id": str(r[0]), "employee_name": str(r[1] or "")} for r in rows if r and r[0]]

    def get_employee_events_utc_range(self, employee_id: str, start_utc_iso: str, end_utc_iso: str) -> List[Dict[str, Any]]:
        emp = (employee_id or "").strip()
        if not emp:
            return []
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    """
                    SELECT je.event_time_utc, je.event_time, je.role, je.jornada_id, j.op_date
                    FROM jornada_events je
                    LEFT JOIN jornadas j ON j.jornada_id = je.jornada_id
                    WHERE je.employee_id = ?
                      AND je.event_time_utc >= ?
                      AND je.event_time_utc < ?
                    ORDER BY je.event_time_utc
                    """,
                    (emp, start_utc_iso, end_utc_iso),
                ).fetchall()
            return [
                {
                    "event_time_utc": str(r[0]),
                    "event_time": str(r[1]),
                    "role": str(r[2]),
                    "jornada_id": str(r[3]),
                    "op_date": str(r[4] or ""),
                }
                for r in rows
            ]
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT je.event_time_utc, je.event_time, je.role, je.jornada_id, j.op_date
            FROM jornada_events je
            LEFT JOIN jornadas j ON j.jornada_id = je.jornada_id
            WHERE je.employee_id = %s
              AND je.event_time_utc >= %s
              AND je.event_time_utc < %s
            ORDER BY je.event_time_utc
            """,
            (emp, start_utc_iso, end_utc_iso),
        )
        rows = cur.fetchall()
        return [
            {
                "event_time_utc": str(r[0]),
                "event_time": str(r[1]),
                "role": str(r[2]),
                "jornada_id": str(r[3]),
                "op_date": str(r[4] or ""),
            }
            for r in rows
        ]

    def list_jornadas_closed_range(self, employee_id: Optional[str], start_utc_iso: str, end_utc_iso: str) -> List[Dict[str, Any]]:
        emp = (employee_id or "").strip()
        if self.engine == "sqlite":
            with self._lock:
                if emp:
                    rows = self._conn.execute(
                        """
                        SELECT employee_id, COALESCE(employee_name,''), op_date, start_time, start_time_utc,
                               end_time, end_time_utc, duration_minutes, events_count, incidencias_json
                        FROM jornadas
                        WHERE employee_id = ?
                          AND closed = 1
                          AND start_time_utc >= ?
                          AND start_time_utc < ?
                        ORDER BY start_time_utc
                        """,
                        (emp, start_utc_iso, end_utc_iso),
                    ).fetchall()
                else:
                    rows = self._conn.execute(
                        """
                        SELECT employee_id, COALESCE(employee_name,''), op_date, start_time, start_time_utc,
                               end_time, end_time_utc, duration_minutes, events_count, incidencias_json
                        FROM jornadas
                        WHERE closed = 1
                          AND start_time_utc >= ?
                          AND start_time_utc < ?
                        ORDER BY employee_id, start_time_utc
                        """,
                        (start_utc_iso, end_utc_iso),
                    ).fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "employee_id": str(r[0]),
                        "employee_name": str(r[1] or ""),
                        "op_date": str(r[2]),
                        "start_time": str(r[3]),
                        "start_time_utc": str(r[4]),
                        "end_time": str(r[5] or ""),
                        "end_time_utc": str(r[6] or ""),
                        "duration_minutes": int(r[7] or 0),
                        "events_count": int(r[8] or 0),
                        "incidencias": json.loads(r[9] or "[]") if r[9] else [],
                    }
                )
            return out
        cur = self._conn.cursor()
        if emp:
            cur.execute(
                """
                SELECT employee_id, COALESCE(employee_name,''), op_date, start_time, start_time_utc,
                       end_time, end_time_utc, duration_minutes, events_count, incidencias_json
                FROM jornadas
                WHERE employee_id = %s
                  AND closed = 1
                  AND start_time_utc >= %s
                  AND start_time_utc < %s
                ORDER BY start_time_utc
                """,
                (emp, start_utc_iso, end_utc_iso),
            )
        else:
            cur.execute(
                """
                SELECT employee_id, COALESCE(employee_name,''), op_date, start_time, start_time_utc,
                       end_time, end_time_utc, duration_minutes, events_count, incidencias_json
                FROM jornadas
                WHERE closed = 1
                  AND start_time_utc >= %s
                  AND start_time_utc < %s
                ORDER BY employee_id, start_time_utc
                """,
                (start_utc_iso, end_utc_iso),
            )
        rows = cur.fetchall()
        out = []
        for r in rows:
            out.append(
                {
                    "employee_id": str(r[0]),
                    "employee_name": str(r[1] or ""),
                    "op_date": str(r[2]),
                    "start_time": str(r[3]),
                    "start_time_utc": str(r[4]),
                    "end_time": str(r[5] or ""),
                    "end_time_utc": str(r[6] or ""),
                    "duration_minutes": int(r[7] or 0),
                    "events_count": int(r[8] or 0),
                    "incidencias": json.loads(r[9] or "[]") if r[9] else [],
                }
            )
        return out

    def upsert_employee_profile(self, employee_id: str, window_days: int, profile: Dict[str, Any]):
        emp = (employee_id or "").strip()
        if not emp:
            return
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    """
                    INSERT INTO rrhh_employee_profile(employee_id, window_days, updated_at, profile_json)
                    VALUES(?,?,?,?)
                    ON CONFLICT(employee_id, window_days) DO UPDATE SET
                      updated_at=excluded.updated_at,
                      profile_json=excluded.profile_json
                    """,
                    (emp, int(window_days), now, json.dumps(profile, ensure_ascii=False)),
                )
                self._conn.commit()
            return
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO rrhh_employee_profile(employee_id, window_days, updated_at, profile_json)
            VALUES(%s,%s,%s,%s)
            ON CONFLICT(employee_id, window_days) DO UPDATE SET
              updated_at=EXCLUDED.updated_at,
              profile_json=EXCLUDED.profile_json
            """,
            (emp, int(window_days), now, json.dumps(profile, ensure_ascii=False)),
        )

    def get_employee_profile(self, employee_id: str, window_days: int) -> Optional[Dict[str, Any]]:
        emp = (employee_id or "").strip()
        if not emp:
            return None
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute(
                    "SELECT profile_json FROM rrhh_employee_profile WHERE employee_id=? AND window_days=?",
                    (emp, int(window_days)),
                ).fetchone()
            if not row:
                return None
            try:
                return json.loads(row[0])
            except Exception:
                return None
        cur = self._conn.cursor()
        cur.execute(
            "SELECT profile_json FROM rrhh_employee_profile WHERE employee_id=%s AND window_days=%s",
            (emp, int(window_days)),
        )
        row = cur.fetchone()
        if not row:
            return None
        try:
            if isinstance(row[0], dict):
                return row[0]
            return json.loads(row[0])
        except Exception:
            return None
