from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class DB:
    def __init__(self, engine: str, sqlite_path: str, postgres_dsn: str = ""):
        self.engine = engine.lower()
        if self.engine not in ("sqlite", "postgres"):
            raise ValueError("database.engine must be 'sqlite' or 'postgres'")

        self.sqlite_path = sqlite_path
        self.postgres_dsn = postgres_dsn
        self._conn = None


        self._lock = threading.RLock()
        self._tx_depth = 0

        if self.engine == "sqlite":
            Path(os.path.dirname(self.sqlite_path)).mkdir(parents=True, exist_ok=True)

            self._conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._init_sqlite()
        else:

            import psycopg
            self._conn = psycopg.connect(self.postgres_dsn, autocommit=True)
            self._init_postgres()

    def close(self):
        if self._conn is not None:
            with self._lock:
                self._conn.close()
                self._conn = None


    def _maybe_commit(self):
        if self.engine == "sqlite" and getattr(self, "_tx_depth", 0) <= 0:
            self._conn.commit()

    @contextmanager
    def transaction(self):
        if self.engine != "sqlite":
            yield
            return
        outer = False
        with self._lock:
            if getattr(self, "_tx_depth", 0) <= 0:
                outer = True
                self._conn.execute("BEGIN")
            self._tx_depth = getattr(self, "_tx_depth", 0) + 1
        try:
            yield
        except Exception:
            with self._lock:
                try:
                    self._conn.rollback()
                finally:
                    self._tx_depth = 0
            raise
        else:
            with self._lock:
                self._tx_depth = max(0, getattr(self, "_tx_depth", 0) - 1)
                if outer:
                    self._conn.commit()


    def _init_sqlite(self):
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  device_ip TEXT NOT NULL,
                  event_time TEXT NOT NULL,
                  event_time_utc TEXT NOT NULL,
                  received_at TEXT NOT NULL,
                  payload_json TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_timeutc ON raw_events(event_time_utc)")

            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  event_uid TEXT NOT NULL UNIQUE,
                  device_ip TEXT NOT NULL,
                  event_date TEXT NOT NULL,
                  event_time TEXT NOT NULL,
                  event_time_utc TEXT NOT NULL,
                  event_type TEXT NOT NULL,
                  employee_id TEXT,
                  employee_name TEXT,
                  payload_json TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_proc_date ON processed_events(event_date)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_proc_timeutc ON processed_events(event_time_utc)")

            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS system_state (
                  k TEXT PRIMARY KEY,
                  v TEXT NOT NULL
                )
                """
            )


            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jornadas (
                  jornada_id TEXT PRIMARY KEY,
                  employee_id TEXT NOT NULL,
                  employee_name TEXT,
                  op_date TEXT NOT NULL,
                  start_time TEXT NOT NULL,
                  start_time_utc TEXT NOT NULL,
                  end_time TEXT,
                  end_time_utc TEXT,
                  duration_minutes INTEGER,
                  events_count INTEGER NOT NULL,
                  incidencias_json TEXT NOT NULL DEFAULT '[]',
                  closed INTEGER NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_jornadas_opdate ON jornadas(op_date)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_jornadas_emp ON jornadas(employee_id, start_time_utc)")

            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jornada_events (
                  event_uid TEXT PRIMARY KEY,
                  jornada_id TEXT NOT NULL,
                  seq INTEGER NOT NULL,
                  role TEXT NOT NULL,
                  event_time TEXT NOT NULL,
                  event_time_utc TEXT NOT NULL,
                  employee_id TEXT NOT NULL,
                  FOREIGN KEY(jornada_id) REFERENCES jornadas(jornada_id) ON DELETE CASCADE
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_je_jornada ON jornada_events(jornada_id, seq)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_je_emp_time ON jornada_events(employee_id, event_time_utc)")

            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS employee_jornada_state (
                  employee_id TEXT PRIMARY KEY,
                  state_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )

            # Model audit (optional) - records borderline decisions (carryback close vs new jornada)
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS model_audit (
                  audit_id TEXT PRIMARY KEY,
                  created_at_utc TEXT NOT NULL,
                  employee_id TEXT NOT NULL,
                  op_date TEXT NOT NULL,
                  event_time_utc TEXT NOT NULL,
                  event_time_local TEXT NOT NULL,
                  boundary_from_op_date TEXT NOT NULL,
                  boundary_to_op_date TEXT NOT NULL,
                  decision TEXT NOT NULL,
                  confidence REAL NOT NULL,
                  p_prior REAL NOT NULL,
                  reasons TEXT NOT NULL,
                  features_json TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_opdate ON model_audit(op_date)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_emp ON model_audit(employee_id, event_time_utc)")
            # Weekly model audit snapshots (per operational week)
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS model_weekly_audit (
                  week_start_op TEXT PRIMARY KEY,
                  week_end_op TEXT NOT NULL,
                  created_at_utc TEXT NOT NULL,
                  max_op_date_used TEXT NOT NULL,
                  peak_mode INTEGER NOT NULL,
                  ioi_mean REAL NOT NULL,
                  ioi_end REAL NOT NULL,
                  jpd_sum INTEGER NOT NULL,
                  d1_rate_mean REAL NOT NULL,
                  cluster_k INTEGER NOT NULL,
                  cluster_counts_json TEXT NOT NULL,
                  notes TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_weekly_end ON model_weekly_audit(week_end_op)")


            # Manual corrections (supervised learning)
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS manual_labels (
                  jornada_uid TEXT PRIMARY KEY,
                  decision TEXT NOT NULL,
                  note TEXT,
                  updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_manual_labels_decision ON manual_labels(decision)")

            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS manual_corrections (
                  id TEXT PRIMARY KEY,
                  export_id TEXT,
                  jornada_uid TEXT NOT NULL,
                  decision TEXT NOT NULL,
                  note TEXT,
                  imported_at_utc TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_manual_corr_juid ON manual_corrections(jornada_uid)")

            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS export_log (
                  export_id TEXT PRIMARY KEY,
                  created_at_utc TEXT NOT NULL,
                  range_start_op TEXT,
                  range_end_op TEXT,
                  source_label TEXT,
                  file_name TEXT
                )
                """
            )

            self._maybe_commit()

    def _init_postgres(self):
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_events (
              id BIGSERIAL PRIMARY KEY,
              device_ip TEXT NOT NULL,
              event_time TEXT NOT NULL,
              event_time_utc TEXT NOT NULL,
              received_at TEXT NOT NULL,
              payload_json JSONB NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_timeutc ON raw_events(event_time_utc)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
              id BIGSERIAL PRIMARY KEY,
              event_uid TEXT NOT NULL UNIQUE,
              device_ip TEXT NOT NULL,
              event_date TEXT NOT NULL,
              event_time TEXT NOT NULL,
              event_time_utc TEXT NOT NULL,
              event_type TEXT NOT NULL,
              employee_id TEXT,
              employee_name TEXT,
              payload_json JSONB NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_proc_date ON processed_events(event_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_proc_timeutc ON processed_events(event_time_utc)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS system_state (
              k TEXT PRIMARY KEY,
              v TEXT NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jornadas (
              jornada_id TEXT PRIMARY KEY,
              employee_id TEXT NOT NULL,
              employee_name TEXT,
              op_date TEXT NOT NULL,
              start_time TEXT NOT NULL,
              start_time_utc TEXT NOT NULL,
              end_time TEXT,
              end_time_utc TEXT,
              duration_minutes INTEGER,
              events_count INTEGER NOT NULL,
              incidencias_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              closed SMALLINT NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jornadas_opdate ON jornadas(op_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jornadas_emp ON jornadas(employee_id, start_time_utc)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jornada_events (
              event_uid TEXT PRIMARY KEY,
              jornada_id TEXT NOT NULL REFERENCES jornadas(jornada_id) ON DELETE CASCADE,
              seq INTEGER NOT NULL,
              role TEXT NOT NULL,
              event_time TEXT NOT NULL,
              event_time_utc TEXT NOT NULL,
              employee_id TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_je_jornada ON jornada_events(jornada_id, seq)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_je_emp_time ON jornada_events(employee_id, event_time_utc)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS employee_jornada_state (
              employee_id TEXT PRIMARY KEY,
              state_json JSONB NOT NULL,
              updated_at TEXT NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS model_audit (
              audit_id TEXT PRIMARY KEY,
              created_at_utc TEXT NOT NULL,
              employee_id TEXT NOT NULL,
              op_date TEXT NOT NULL,
              event_time_utc TEXT NOT NULL,
              event_time_local TEXT NOT NULL,
              boundary_from_op_date TEXT NOT NULL,
              boundary_to_op_date TEXT NOT NULL,
              decision TEXT NOT NULL,
              confidence DOUBLE PRECISION NOT NULL,
              p_prior DOUBLE PRECISION NOT NULL,
              reasons TEXT NOT NULL,
              features_json JSONB NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_opdate ON model_audit(op_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_emp ON model_audit(employee_id, event_time_utc)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS model_weekly_audit (
              week_start_op TEXT PRIMARY KEY,
              week_end_op TEXT NOT NULL,
              created_at_utc TEXT NOT NULL,
              max_op_date_used TEXT NOT NULL,
              peak_mode INTEGER NOT NULL,
              ioi_mean DOUBLE PRECISION NOT NULL,
              ioi_end DOUBLE PRECISION NOT NULL,
              jpd_sum BIGINT NOT NULL,
              d1_rate_mean DOUBLE PRECISION NOT NULL,
              cluster_k INTEGER NOT NULL,
              cluster_counts_json JSONB NOT NULL,
              notes TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_end ON model_weekly_audit(week_end_op)")


        # Manual corrections (supervised learning)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_labels (
              jornada_uid TEXT PRIMARY KEY,
              decision TEXT NOT NULL,
              note TEXT,
              updated_at TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_manual_labels_decision ON manual_labels(decision)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_corrections (
              id TEXT PRIMARY KEY,
              export_id TEXT,
              jornada_uid TEXT NOT NULL,
              decision TEXT NOT NULL,
              note TEXT,
              imported_at_utc TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_manual_corr_juid ON manual_corrections(jornada_uid)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS export_log (
              export_id TEXT PRIMARY KEY,
              created_at_utc TEXT NOT NULL,
              range_start_op TEXT,
              range_end_op TEXT,
              source_label TEXT,
              file_name TEXT
            );
            """
        )

        cur.close()


    def upsert_state(self, k: str, v: str):
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO system_state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                    (k, v),
                )
                self._maybe_commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO system_state(k,v) VALUES(%s,%s) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v",
                    (k, v),
                )

    def get_state(self, k: str) -> Optional[str]:
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute("SELECT v FROM system_state WHERE k=?", (k,)).fetchone()
                return row[0] if row else None
        else:
            with self._conn.cursor() as cur:
                cur.execute("SELECT v FROM system_state WHERE k=%s", (k,))
                row = cur.fetchone()
                return row[0] if row else None


    def insert_raw(self, device_ip: str, event_time: str, event_time_utc: str, payload: Dict[str, Any]):
        received_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO raw_events(device_ip,event_time,event_time_utc,received_at,payload_json) VALUES(?,?,?,?,?)",
                    (device_ip, event_time, event_time_utc, received_at, json.dumps(payload, ensure_ascii=False)),
                )
                self._maybe_commit()
        else:
            from psycopg.types.json import Json
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO raw_events(device_ip,event_time,event_time_utc,received_at,payload_json) VALUES(%s,%s,%s,%s,%s)",
                    (device_ip, event_time, event_time_utc, received_at, Json(payload)),
                )

    def insert_processed(
        self,
        event_uid: str,
        device_ip: str,
        event_date: str,
        event_time: str,
        event_time_utc: str,
        event_type: str,
        employee_id: Optional[str],
        employee_name: Optional[str],
        payload: Dict[str, Any],
    ) -> bool:

        if self.engine == "sqlite":
            with self._lock:
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO processed_events(event_uid,device_ip,event_date,event_time,event_time_utc,event_type,employee_id,employee_name,payload_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        event_uid,
                        device_ip,
                        event_date,
                        event_time,
                        event_time_utc,
                        event_type,
                        employee_id,
                        employee_name,
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
                self._maybe_commit()
                return cur.rowcount == 1
        else:
            from psycopg.types.json import Json
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO processed_events(event_uid,device_ip,event_date,event_time,event_time_utc,event_type,employee_id,employee_name,payload_json) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(event_uid) DO NOTHING",
                    (
                        event_uid,
                        device_ip,
                        event_date,
                        event_time,
                        event_time_utc,
                        event_type,
                        employee_id,
                        employee_name,
                        Json(payload),
                    ),
                )
                return cur.rowcount == 1


    def sqlite_backup_if_needed(self, backup_dir: str, maxdbsize_mb: int):
        if self.engine != 'sqlite':
            return
        if self.sqlite_file_size_mb() <= float(maxdbsize_mb):
            return
        self.backup_sqlite(backup_dir)

    def count_raw(self) -> int:
        if self.engine == "sqlite":
            row = self._conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()
            return int(row[0] or 0)
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_events")
            row = cur.fetchone()
            return int(row[0] or 0)

    def count_processed(self) -> int:
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()
                return int(row[0] or 0)
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM processed_events")
            row = cur.fetchone()
            return int(row[0] or 0)

    def count_raw_by_date(self, yyyy_mm_dd: str) -> int:
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM raw_events WHERE substr(event_time,1,10)=?",
                    (yyyy_mm_dd,),
                ).fetchone()
                return int(row[0] or 0)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw_events WHERE substring(event_time,1,10)=%s",
                (yyyy_mm_dd,),
            )
            row = cur.fetchone()
            return int(row[0] or 0)

    def count_processed_by_date(self, yyyy_mm_dd: str) -> int:
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM processed_events WHERE event_date=?",
                    (yyyy_mm_dd,),
                ).fetchone()
                return int(row[0] or 0)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_date=%s",
                (yyyy_mm_dd,),
            )
            row = cur.fetchone()
            return int(row[0] or 0)

    def get_verify_mode_counts_raw_by_date(self, yyyy_mm_dd: str) -> Dict[str, int]:

        if self.engine == "sqlite":
            try:
                row = self._conn.execute(
                    """
                    SELECT
                      COUNT(*) AS total,
                      SUM(
                        CASE
                          WHEN lower(
                            COALESCE(
                              json_extract(payload_json, '$.verify_mode'),
                              json_extract(payload_json, '$.currentVerifyMode'),
                              ''
                            )
                          ) = 'invalid' THEN 1 ELSE 0
                        END
                      ) AS invalid_count
                    FROM raw_events
                    WHERE substr(event_time,1,10)=?
                    """,
                    (yyyy_mm_dd,),
                ).fetchone()
                total = int(row[0] or 0)
                invalid = int(row[1] or 0)
                return {"total": total, "invalid": invalid, "valid": max(0, total - invalid)}
            except Exception:
                return {"total": -1, "invalid": -1, "valid": -1}

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) AS total,
                  SUM(
                    CASE
                      WHEN lower(COALESCE(payload_json->>'verify_mode', payload_json->>'currentVerifyMode', '')) = 'invalid'
                      THEN 1 ELSE 0
                    END
                  ) AS invalid_count
                FROM raw_events
                WHERE substring(event_time,1,10)=%s
                """,
                (yyyy_mm_dd,),
            )
            row = cur.fetchone()
            total = int(row[0] or 0)
            invalid = int(row[1] or 0)
            return {"total": total, "invalid": invalid, "valid": max(0, total - invalid)}


    def get_raw_by_date(self, yyyy_mm_dd: str) -> List[Dict[str, Any]]:

        if self.engine == "sqlite":
            rows = self._conn.execute(
                "SELECT device_ip,event_time,event_time_utc,received_at,payload_json "
                "FROM raw_events WHERE substr(event_time,1,10)=? ORDER BY event_time_utc",
                (yyyy_mm_dd,),
            ).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT device_ip,event_time,event_time_utc,received_at,payload_json "
                    "FROM raw_events WHERE substring(event_time,1,10)=%s ORDER BY event_time_utc",
                    (yyyy_mm_dd,),
                )
                rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for device_ip, event_time, event_time_utc, received_at, payload_json in rows:
            payload = payload_json if isinstance(payload_json, dict) else json.loads(payload_json)
            payload.setdefault("device_ip", device_ip)
            payload.setdefault("event_time", event_time)
            payload.setdefault("event_time_utc", event_time_utc)
            payload.setdefault("received_at", received_at)
            out.append(payload)
        return out

    def get_processed_by_date(self, yyyy_mm_dd: str) -> List[Dict[str, Any]]:
        if self.engine == "sqlite":
            rows = self._conn.execute(
                "SELECT event_uid,device_ip,event_date,event_time,event_time_utc,event_type,employee_id,employee_name,payload_json "
                "FROM processed_events WHERE event_date=? ORDER BY event_time_utc",
                (yyyy_mm_dd,),
            ).fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "event_uid": r[0],
                        "device_ip": r[1],
                        "event_date": r[2],
                        "event_time": r[3],
                        "event_time_utc": r[4],
                        "event_type": r[5],
                        "employee_id": r[6],
                        "employee_name": r[7],
                        "payload": json.loads(r[8]),
                    }
                )
            return out

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT event_uid,device_ip,event_date,event_time,event_time_utc,event_type,employee_id,employee_name,payload_json "
                "FROM processed_events WHERE event_date=%s ORDER BY event_time_utc",
                (yyyy_mm_dd,),
            )
            rows = cur.fetchall()
            return [
                {
                    "event_uid": r[0],
                    "device_ip": r[1],
                    "event_date": r[2],
                    "event_time": r[3],
                    "event_time_utc": r[4],
                    "event_type": r[5],
                    "employee_id": r[6],
                    "employee_name": r[7],
                    "payload": r[8],
                }
                for r in rows
            ]


    def get_raw_by_utc_range(self, start_utc_iso: str, end_utc_iso: str) -> List[Dict[str, Any]]:

        if self.engine == "sqlite":
            rows = self._conn.execute(
                "SELECT device_ip,event_time,event_time_utc,received_at,payload_json "
                "FROM raw_events WHERE event_time_utc>=? AND event_time_utc<? ORDER BY event_time_utc",
                (start_utc_iso, end_utc_iso),
            ).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT device_ip,event_time,event_time_utc,received_at,payload_json "
                    "FROM raw_events WHERE event_time_utc>=%s AND event_time_utc<%s ORDER BY event_time_utc",
                    (start_utc_iso, end_utc_iso),
                )
                rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for device_ip, event_time, event_time_utc, received_at, payload_json in rows:
            payload = payload_json if isinstance(payload_json, dict) else json.loads(payload_json)
            payload.setdefault("device_ip", device_ip)
            payload.setdefault("event_time", event_time)
            payload.setdefault("event_time_utc", event_time_utc)
            payload.setdefault("received_at", received_at)
            out.append(payload)
        return out

    def get_processed_by_utc_range(self, start_utc_iso: str, end_utc_iso: str) -> List[Dict[str, Any]]:
        if self.engine == "sqlite":
            rows = self._conn.execute(
                "SELECT event_uid,device_ip,event_date,event_time,event_time_utc,event_type,employee_id,employee_name,payload_json "
                "FROM processed_events WHERE event_time_utc>=? AND event_time_utc<? ORDER BY event_time_utc",
                (start_utc_iso, end_utc_iso),
            ).fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "event_uid": r[0],
                        "device_ip": r[1],
                        "event_date": r[2],
                        "event_time": r[3],
                        "event_time_utc": r[4],
                        "event_type": r[5],
                        "employee_id": r[6],
                        "employee_name": r[7],
                        "payload": json.loads(r[8]),
                    }
                )
            return out

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT event_uid,device_ip,event_date,event_time,event_time_utc,event_type,employee_id,employee_name,payload_json "
                "FROM processed_events WHERE event_time_utc>=%s AND event_time_utc<%s ORDER BY event_time_utc",
                (start_utc_iso, end_utc_iso),
            )
            rows = cur.fetchall()
            return [
                {
                    "event_uid": r[0],
                    "device_ip": r[1],
                    "event_date": r[2],
                    "event_time": r[3],
                    "event_time_utc": r[4],
                    "event_type": r[5],
                    "employee_id": r[6],
                    "employee_name": r[7],
                    "payload": r[8],
                }
                for r in rows
            ]


    def get_last_raw_event_time(self) -> Optional[str]:
        if self.engine == "sqlite":
            row = self._conn.execute("SELECT event_time FROM raw_events ORDER BY event_time_utc DESC LIMIT 1").fetchone()
            return row[0] if row else None
        with self._conn.cursor() as cur:
            cur.execute("SELECT event_time FROM raw_events ORDER BY event_time_utc DESC LIMIT 1")
            row = cur.fetchone()
            return row[0] if row else None

    def get_last_processed_event_time(self) -> Optional[str]:
        if self.engine == "sqlite":
            row = self._conn.execute("SELECT event_time FROM processed_events ORDER BY event_time_utc DESC LIMIT 1").fetchone()
            return row[0] if row else None
        with self._conn.cursor() as cur:
            cur.execute("SELECT event_time FROM processed_events ORDER BY event_time_utc DESC LIMIT 1")
            row = cur.fetchone()
            return row[0] if row else None


    def prune_old(self, keep_raw_days: int, keep_processed_days: int):
        now = datetime.utcnow()
        raw_cut = (now - timedelta(days=keep_raw_days)).replace(microsecond=0).isoformat() + "Z"
        proc_cut = (now - timedelta(days=keep_processed_days)).replace(microsecond=0).isoformat() + "Z"

        if self.engine == "sqlite":
            self._conn.execute("DELETE FROM raw_events WHERE event_time_utc < ?", (raw_cut,))
            self._conn.execute("DELETE FROM processed_events WHERE event_time_utc < ?", (proc_cut,))
            self._maybe_commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute("DELETE FROM raw_events WHERE event_time_utc < %s", (raw_cut,))
                cur.execute("DELETE FROM processed_events WHERE event_time_utc < %s", (proc_cut,))

    def sqlite_file_size_mb(self) -> float:
        if self.engine != "sqlite":
            return 0.0
        try:
            return os.path.getsize(self.sqlite_path) / (1024 * 1024)
        except FileNotFoundError:
            return 0.0

    def backup_sqlite(self, backup_dir: str) -> Optional[str]:
        if self.engine != 'sqlite':
            return None
        Path(backup_dir).mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        dst = Path(backup_dir) / f'collector_{ts}.sqlite'
        tmp = Path(str(dst) + '.tmp')
        with self._lock:
            self._maybe_commit()
            dst_conn = sqlite3.connect(str(tmp))
            try:
                self._conn.backup(dst_conn)
                dst_conn.execute('PRAGMA journal_mode=DELETE;')
                dst_conn.commit()
            finally:
                dst_conn.close()
        os.replace(str(tmp), str(dst))
        return str(dst)

    

    def get_last_processed_event_time_utc(self) -> Optional[str]:
        """Return last processed event_time_utc (ISO Z) or None."""
        if self.engine == "sqlite":
            row = self._conn.execute(
                "SELECT event_time_utc FROM processed_events ORDER BY event_time_utc DESC LIMIT 1"
            ).fetchone()
            return row[0] if row else None
        with self._conn.cursor() as cur:
            cur.execute("SELECT event_time_utc FROM processed_events ORDER BY event_time_utc DESC LIMIT 1")
            row = cur.fetchone()
            return row[0] if row else None
    def clear_jornadas(self, preserve_patterns: bool = True):
        """Clear computed jornadas/jornada_events.

        By default, this preserves learned per-employee state/patterns stored in
        employee_jornada_state. Set preserve_patterns=False to wipe patterns too.
        """

        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute("DELETE FROM jornada_events")
                self._conn.execute("DELETE FROM jornadas")
                if not preserve_patterns:
                    self._conn.execute("DELETE FROM employee_jornada_state")
                self._maybe_commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute("DELETE FROM jornada_events")
                cur.execute("DELETE FROM jornadas")
                if not preserve_patterns:
                    cur.execute("DELETE FROM employee_jornada_state")


    def reset_employee_state_preserve_patterns(self) -> int:
        """Reset transient jornada-indexing state per employee, keeping learned patterns.

        Why: a rebuild replays events from the beginning. If we keep the *transient* state
        (current_jornada_id, expected_role, seq, last_event, etc.) from a later point in time,
        the replay can misclassify boundaries. This function clears only the volatile fields,
        preserving any learned keys (patterns / model profiles / cluster id, etc.).

        Returns the number of employee states updated.
        """
        import json
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        volatile_keys = {
            "current_jornada_id",
            "current_start_utc",
            "current_start_local",
            "current_op_date",
            "expected_role",
            "seq",
            "last_event_utc",
            "last_event_local",
            "incidencias",
            "has_late_signature",
        }

        def _reset(st: dict) -> dict:
            if not isinstance(st, dict):
                st = {}
            for k in volatile_keys:
                if k in st:
                    del st[k]
            # Safe defaults
            st["expected_role"] = "IN"
            st["seq"] = 0
            st["last_event_utc"] = ""
            st["last_event_local"] = ""
            st["incidencias"] = []
            st["has_late_signature"] = False
            return st

        updated = 0
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute("SELECT employee_id,state_json FROM employee_jornada_state").fetchall()
                for employee_id, state_json in rows or []:
                    try:
                        st = json.loads(state_json) if state_json else {}
                        st2 = _reset(st)
                        self._conn.execute(
                            "UPDATE employee_jornada_state SET state_json=?, updated_at=? WHERE employee_id=?",
                            (json.dumps(st2, ensure_ascii=False), now, employee_id),
                        )
                        updated += 1
                    except Exception:
                        continue
                self._maybe_commit()
            return updated

        with self._conn.cursor() as cur:
            cur.execute("SELECT employee_id,state_json FROM employee_jornada_state")
            rows = cur.fetchall()
            for employee_id, state_json in rows or []:
                try:
                    st = state_json if isinstance(state_json, dict) else json.loads(state_json)
                    st2 = _reset(st)
                    cur.execute(
                        "UPDATE employee_jornada_state SET state_json=%s, updated_at=%s WHERE employee_id=%s",
                        (json.dumps(st2, ensure_ascii=False), now, employee_id),
                    )
                    updated += 1
                except Exception:
                    continue
        return updated

    def upsert_employee_jornada_state(self, employee_id: str, state: Dict[str, Any]):
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO employee_jornada_state(employee_id,state_json,updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(employee_id) DO UPDATE SET state_json=excluded.state_json, updated_at=excluded.updated_at",
                    (employee_id, json.dumps(state, ensure_ascii=False), now),
                )
                self._maybe_commit()
        else:
            from psycopg.types.json import Json
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO employee_jornada_state(employee_id,state_json,updated_at) VALUES(%s,%s,%s) "
                    "ON CONFLICT(employee_id) DO UPDATE SET state_json=EXCLUDED.state_json, updated_at=EXCLUDED.updated_at",
                    (employee_id, Json(state), now),
                )

    def get_employee_jornada_state(self, employee_id: str) -> Optional[Dict[str, Any]]:
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute(
                    "SELECT state_json FROM employee_jornada_state WHERE employee_id=?",
                    (employee_id,),
                ).fetchone()
        else:
            with self._conn.cursor() as cur:
                cur.execute("SELECT state_json FROM employee_jornada_state WHERE employee_id=%s", (employee_id,))
                row = cur.fetchone()
        if not row:
            return None
        payload_json = row[0]
        return payload_json if isinstance(payload_json, dict) else json.loads(payload_json)


    # --- Model audit (optional) ---
    def insert_model_audit(self, rec: Dict[str, Any]):
        """Insert an audit record for borderline decisions.

        Expected keys:
          employee_id, op_date, event_time_utc, event_time_local,
          boundary_from_op_date, boundary_to_op_date,
          decision, confidence, p_prior, reasons, features
        """
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        audit_id = rec.get("audit_id") or str(uuid.uuid4())
        features = rec.get("features") or {}
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT OR REPLACE INTO model_audit(audit_id,created_at_utc,employee_id,op_date,event_time_utc,event_time_local,boundary_from_op_date,boundary_to_op_date,decision,confidence,p_prior,reasons,features_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        audit_id,
                        now,
                        rec.get("employee_id") or "",
                        rec.get("op_date") or "",
                        rec.get("event_time_utc") or "",
                        rec.get("event_time_local") or "",
                        rec.get("boundary_from_op_date") or "",
                        rec.get("boundary_to_op_date") or "",
                        rec.get("decision") or "",
                        float(rec.get("confidence") or 0.0),
                        float(rec.get("p_prior") or 0.0),
                        rec.get("reasons") or "",
                        json.dumps(features, ensure_ascii=False),
                    ),
                )
                self._maybe_commit()
        else:
            from psycopg.types.json import Json
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO model_audit(audit_id,created_at_utc,employee_id,op_date,event_time_utc,event_time_local,boundary_from_op_date,boundary_to_op_date,decision,confidence,p_prior,reasons,features_json) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(audit_id) DO UPDATE SET created_at_utc=EXCLUDED.created_at_utc, employee_id=EXCLUDED.employee_id, op_date=EXCLUDED.op_date, event_time_utc=EXCLUDED.event_time_utc, event_time_local=EXCLUDED.event_time_local, boundary_from_op_date=EXCLUDED.boundary_from_op_date, boundary_to_op_date=EXCLUDED.boundary_to_op_date, decision=EXCLUDED.decision, confidence=EXCLUDED.confidence, p_prior=EXCLUDED.p_prior, reasons=EXCLUDED.reasons, features_json=EXCLUDED.features_json",
                    (
                        audit_id,
                        now,
                        rec.get("employee_id") or "",
                        rec.get("op_date") or "",
                        rec.get("event_time_utc") or "",
                        rec.get("event_time_local") or "",
                        rec.get("boundary_from_op_date") or "",
                        rec.get("boundary_to_op_date") or "",
                        rec.get("decision") or "",
                        float(rec.get("confidence") or 0.0),
                        float(rec.get("p_prior") or 0.0),
                        rec.get("reasons") or "",
                        Json(features),
                    ),
                )

    def get_model_audit_range(self, start_op_date: str, end_op_date: str) -> List[Dict[str, Any]]:
        """Fetch audit rows for op_date inclusive range."""
        if not start_op_date or not end_op_date:
            return []
        rows: List[Any]
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    "SELECT created_at_utc,employee_id,op_date,event_time_local,decision,confidence,p_prior,reasons,features_json,boundary_to_op_date "
                    "FROM model_audit WHERE op_date>=? AND op_date<=? ORDER BY op_date,event_time_utc",
                    (start_op_date, end_op_date),
                ).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT created_at_utc,employee_id,op_date,event_time_local,decision,confidence,p_prior,reasons,features_json,boundary_to_op_date "
                    "FROM model_audit WHERE op_date>=%s AND op_date<=%s ORDER BY op_date,event_time_utc",
                    (start_op_date, end_op_date),
                )
                rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            created_at_utc, employee_id, op_date, event_time_local, decision, confidence, p_prior, reasons, features_json, boundary_to_op_date = r
            features = features_json if isinstance(features_json, dict) else json.loads(features_json or "{}")
            out.append(
                {
                    "created_at_utc": created_at_utc,
                    "employee_id": employee_id,
                    "op_date": op_date,
                    "boundary_to_op_date": boundary_to_op_date,
                    "event_time_local": event_time_local,
                    "decision": decision,
                    "confidence": float(confidence or 0.0),
                    "p_prior": float(p_prior or 0.0),
                    "reasons": reasons,
                    "seq_before": features.get("seq_before"),
                    "min_prev": features.get("min_prev"),
                    "allow_no_late": features.get("allow_no_late"),
                    "has_late_signature": features.get("has_late_signature"),
                    "lookahead_cnt": features.get("lookahead_cnt"),
                }
            )
        return out

    def prune_model_audit(self, keep_days: int = 30):
        """Delete audit records older than keep_days (by created_at_utc)."""
        keep_days = int(keep_days or 0)
        if keep_days <= 0:
            return
        cutoff = (datetime.utcnow() - timedelta(days=keep_days)).replace(microsecond=0).isoformat() + "Z"
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute("DELETE FROM model_audit WHERE created_at_utc < ?", (cutoff,))
                self._maybe_commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute("DELETE FROM model_audit WHERE created_at_utc < %s", (cutoff,))

    def count_employee_jornada_states(self) -> int:
        """How many employees have saved state (patterns/model)."""
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute("SELECT COUNT(*) FROM employee_jornada_state").fetchone()
                return int(row[0] or 0) if row else 0
        else:
            with self._conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM employee_jornada_state")
                row = cur.fetchone()
                return int(row[0] or 0) if row else 0

    def count_model_audit(self) -> int:
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute("SELECT COUNT(*) FROM model_audit").fetchone()
                return int(row[0] or 0) if row else 0
        else:
            with self._conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM model_audit")
                row = cur.fetchone()
                return int(row[0] or 0) if row else 0


    def upsert_weekly_audit(self, rec: Dict[str, Any]):
        """Upsert a weekly model snapshot (keyed by week_start_op)."""
        if not isinstance(rec, dict):
            return
        week_start = str(rec.get("week_start_op") or "").strip()
        if not week_start:
            return
        week_end = str(rec.get("week_end_op") or "").strip()
        created_at = str(rec.get("created_at_utc") or "").strip() or (datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
        max_used = str(rec.get("max_op_date_used") or "").strip() or week_end
        peak_mode = int(bool(rec.get("peak_mode", False)))
        ioi_mean = float(rec.get("ioi_mean") or 0.0)
        ioi_end = float(rec.get("ioi_end") or 0.0)
        jpd_sum = int(rec.get("jpd_sum") or 0)
        d1_rate_mean = float(rec.get("d1_rate_mean") or 0.0)
        cluster_k = int(rec.get("cluster_k") or 0)
        cluster_counts = rec.get("cluster_counts") or {}
        notes = str(rec.get("notes") or "")

        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT OR REPLACE INTO model_weekly_audit(week_start_op,week_end_op,created_at_utc,max_op_date_used,peak_mode,ioi_mean,ioi_end,jpd_sum,d1_rate_mean,cluster_k,cluster_counts_json,notes) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        week_start,
                        week_end,
                        created_at,
                        max_used,
                        peak_mode,
                        ioi_mean,
                        ioi_end,
                        jpd_sum,
                        d1_rate_mean,
                        cluster_k,
                        json.dumps(cluster_counts, ensure_ascii=False),
                        notes,
                    ),
                )
                self._maybe_commit()
        else:
            from psycopg.types.json import Json
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO model_weekly_audit(week_start_op,week_end_op,created_at_utc,max_op_date_used,peak_mode,ioi_mean,ioi_end,jpd_sum,d1_rate_mean,cluster_k,cluster_counts_json,notes) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(week_start_op) DO UPDATE SET "
                    "week_end_op=EXCLUDED.week_end_op, created_at_utc=EXCLUDED.created_at_utc, max_op_date_used=EXCLUDED.max_op_date_used, "
                    "peak_mode=EXCLUDED.peak_mode, ioi_mean=EXCLUDED.ioi_mean, ioi_end=EXCLUDED.ioi_end, "
                    "jpd_sum=EXCLUDED.jpd_sum, d1_rate_mean=EXCLUDED.d1_rate_mean, cluster_k=EXCLUDED.cluster_k, "
                    "cluster_counts_json=EXCLUDED.cluster_counts_json, notes=EXCLUDED.notes",
                    (
                        week_start,
                        week_end,
                        created_at,
                        max_used,
                        peak_mode,
                        ioi_mean,
                        ioi_end,
                        jpd_sum,
                        d1_rate_mean,
                        cluster_k,
                        Json(cluster_counts),
                        notes,
                    ),
                )

    def get_last_weekly_audit(self) -> Optional[Dict[str, Any]]:
        """Return the most recent weekly audit snapshot (by week_start_op)."""
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute(
                    "SELECT week_start_op,week_end_op,created_at_utc,max_op_date_used,peak_mode,ioi_mean,ioi_end,jpd_sum,d1_rate_mean,cluster_k,cluster_counts_json,notes "
                    "FROM model_weekly_audit ORDER BY week_start_op DESC LIMIT 1"
                ).fetchone()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT week_start_op,week_end_op,created_at_utc,max_op_date_used,peak_mode,ioi_mean,ioi_end,jpd_sum,d1_rate_mean,cluster_k,cluster_counts_json,notes "
                    "FROM model_weekly_audit ORDER BY week_start_op DESC LIMIT 1"
                )
                row = cur.fetchone()
        if not row:
            return None
        try:
            cluster_counts = json.loads(row[10]) if isinstance(row[10], str) else (row[10] or {})
        except Exception:
            cluster_counts = {}
        return {
            "week_start_op": str(row[0]),
            "week_end_op": str(row[1]),
            "created_at_utc": str(row[2]),
            "max_op_date_used": str(row[3]),
            "peak_mode": bool(int(row[4] or 0)),
            "ioi_mean": float(row[5] or 0.0),
            "ioi_end": float(row[6] or 0.0),
            "jpd_sum": int(row[7] or 0),
            "d1_rate_mean": float(row[8] or 0.0),
            "cluster_k": int(row[9] or 0),
            "cluster_counts": cluster_counts if isinstance(cluster_counts, dict) else {},
            "notes": str(row[11] or ""),
        }

    def weekly_audit_exists(self, week_start_op: str) -> bool:
        week_start_op = str(week_start_op or "").strip()
        if not week_start_op:
            return False
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute(
                    "SELECT 1 FROM model_weekly_audit WHERE week_start_op=? LIMIT 1",
                    (week_start_op,),
                ).fetchone()
                return row is not None
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM model_weekly_audit WHERE week_start_op=%s LIMIT 1",
                    (week_start_op,),
                )
                row = cur.fetchone()
                return row is not None



    def has_jornada(self, jornada_id: str) -> bool:
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute(
                    "SELECT 1 FROM jornadas WHERE jornada_id=? LIMIT 1",
                    (jornada_id,),
                ).fetchone()
                return row is not None
        else:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1 FROM jornadas WHERE jornada_id=%s LIMIT 1", (jornada_id,))
                row = cur.fetchone()
                return row is not None

    def upsert_jornada(self, jornada: Dict[str, Any]):

        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        incidencias = jornada.get("incidencias") or []
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO jornadas(jornada_id,employee_id,employee_name,op_date,start_time,start_time_utc,end_time,end_time_utc,duration_minutes,events_count,incidencias_json,closed,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(jornada_id) DO UPDATE SET "
                    "employee_id=excluded.employee_id, employee_name=excluded.employee_name, op_date=excluded.op_date, "
                    "start_time=excluded.start_time, start_time_utc=excluded.start_time_utc, "
                    "end_time=excluded.end_time, end_time_utc=excluded.end_time_utc, "
                    "duration_minutes=excluded.duration_minutes, events_count=excluded.events_count, "
                    "incidencias_json=excluded.incidencias_json, closed=excluded.closed, updated_at=excluded.updated_at",
                    (
                        jornada["jornada_id"],
                        jornada["employee_id"],
                        jornada.get("employee_name"),
                        jornada["op_date"],
                        jornada["start_time"],
                        jornada["start_time_utc"],
                        jornada.get("end_time"),
                        jornada.get("end_time_utc"),
                        jornada.get("duration_minutes"),
                        int(jornada.get("events_count") or 0),
                        json.dumps(incidencias, ensure_ascii=False),
                        int(jornada.get("closed") or 0),
                        now,
                    ),
                )
                self._maybe_commit()
        else:
            from psycopg.types.json import Json
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO jornadas(jornada_id,employee_id,employee_name,op_date,start_time,start_time_utc,end_time,end_time_utc,duration_minutes,events_count,incidencias_json,closed,updated_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(jornada_id) DO UPDATE SET "
                    "employee_id=EXCLUDED.employee_id, employee_name=EXCLUDED.employee_name, op_date=EXCLUDED.op_date, "
                    "start_time=EXCLUDED.start_time, start_time_utc=EXCLUDED.start_time_utc, "
                    "end_time=EXCLUDED.end_time, end_time_utc=EXCLUDED.end_time_utc, "
                    "duration_minutes=EXCLUDED.duration_minutes, events_count=EXCLUDED.events_count, "
                    "incidencias_json=EXCLUDED.incidencias_json, closed=EXCLUDED.closed, updated_at=EXCLUDED.updated_at",
                    (
                        jornada["jornada_id"],
                        jornada["employee_id"],
                        jornada.get("employee_name"),
                        jornada["op_date"],
                        jornada["start_time"],
                        jornada["start_time_utc"],
                        jornada.get("end_time"),
                        jornada.get("end_time_utc"),
                        jornada.get("duration_minutes"),
                        int(jornada.get("events_count") or 0),
                        Json(incidencias),
                        int(jornada.get("closed") or 0),
                        now,
                    ),
                )

    def upsert_jornada_event(self, ev: Dict[str, Any]):
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO jornada_events(event_uid,jornada_id,seq,role,event_time,event_time_utc,employee_id) "
                    "VALUES(?,?,?,?,?,?,?) "
                    "ON CONFLICT(event_uid) DO UPDATE SET "
                    "jornada_id=excluded.jornada_id, seq=excluded.seq, role=excluded.role, "
                    "event_time=excluded.event_time, event_time_utc=excluded.event_time_utc, employee_id=excluded.employee_id",
                    (
                        ev["event_uid"],
                        ev["jornada_id"],
                        int(ev["seq"]),
                        ev["role"],
                        ev["event_time"],
                        ev["event_time_utc"],
                        ev["employee_id"],
                    ),
                )
                self._maybe_commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO jornada_events(event_uid,jornada_id,seq,role,event_time,event_time_utc,employee_id) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(event_uid) DO UPDATE SET "
                    "jornada_id=EXCLUDED.jornada_id, seq=EXCLUDED.seq, role=EXCLUDED.role, "
                    "event_time=EXCLUDED.event_time, event_time_utc=EXCLUDED.event_time_utc, employee_id=EXCLUDED.employee_id",
                    (
                        ev["event_uid"],
                        ev["jornada_id"],
                        int(ev["seq"]),
                        ev["role"],
                        ev["event_time"],
                        ev["event_time_utc"],
                        ev["employee_id"],
                    ),
                )

    def delete_jornada(self, jornada_id: str):
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute("DELETE FROM jornadas WHERE jornada_id=?", (jornada_id,))
                self._maybe_commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute("DELETE FROM jornadas WHERE jornada_id=%s", (jornada_id,))

    def get_jornadas_by_op_date(self, op_date: str) -> List[Dict[str, Any]]:

        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    "SELECT jornada_id,employee_id,employee_name,op_date,start_time,start_time_utc,end_time,end_time_utc,duration_minutes,events_count,incidencias_json,closed "
                    "FROM jornadas WHERE op_date=? ORDER BY employee_id, start_time_utc",
                    (op_date,),
                ).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT jornada_id,employee_id,employee_name,op_date,start_time,start_time_utc,end_time,end_time_utc,duration_minutes,events_count,incidencias_json,closed "
                    "FROM jornadas WHERE op_date=%s ORDER BY employee_id, start_time_utc",
                    (op_date,),
                )
                rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            jornada_id, employee_id, employee_name, op_date, start_time, start_time_utc, end_time, end_time_utc, dur, cnt, inc_json, closed = r
            inc = inc_json if isinstance(inc_json, list) else json.loads(inc_json or "[]")
            events = self.get_jornada_events(jornada_id)
            out.append(
                {
                    "jornada_id": jornada_id,
                    "employee_id": employee_id,
                    "employee_name": employee_name,
                    "op_date": op_date,
                    "start_time": start_time,
                    "start_time_utc": start_time_utc,
                    "end_time": end_time,
                    "end_time_utc": end_time_utc,
                    "duration_minutes": dur,
                    "events_count": cnt,
                    "incidencias": inc,
                    "closed": int(closed or 0),
                    "events": events,
                }
            )
        return out

    def get_jornada_events(self, jornada_id: str) -> List[Dict[str, Any]]:
        if self.engine == "sqlite":
            with self._lock:
                rows = self._conn.execute(
                    "SELECT event_uid,seq,role,event_time,event_time_utc,employee_id FROM jornada_events WHERE jornada_id=? ORDER BY seq",
                    (jornada_id,),
                ).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT event_uid,seq,role,event_time,event_time_utc,employee_id FROM jornada_events WHERE jornada_id=%s ORDER BY seq",
                    (jornada_id,),
                )
                rows = cur.fetchall()
        evs: List[Dict[str, Any]] = []
        for event_uid, seq, role, event_time, event_time_utc, employee_id in rows:
            evs.append(
                {
                    "event_uid": event_uid,
                    "seq": int(seq),
                    "role": role,
                    "event_time": event_time,
                    "event_time_utc": event_time_utc,
                    "employee_id": employee_id,
                }
            )
        return evs


    def upsert_manual_label(self, jornada_uid: str, decision: str, note: str = "") -> None:
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO manual_labels(jornada_uid,decision,note,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(jornada_uid) DO UPDATE SET decision=excluded.decision, note=excluded.note, updated_at=excluded.updated_at",
                    (jornada_uid, decision, note, now),
                )
                self._maybe_commit()
            return
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO manual_labels(jornada_uid,decision,note,updated_at) VALUES(%s,%s,%s,%s) "
                "ON CONFLICT (jornada_uid) DO UPDATE SET decision=EXCLUDED.decision, note=EXCLUDED.note, updated_at=EXCLUDED.updated_at",
                (jornada_uid, decision, note, now),
            )

    def get_manual_label(self, jornada_uid: str) -> Optional[str]:
        if not jornada_uid:
            return None
        if self.engine == "sqlite":
            with self._lock:
                row = self._conn.execute(
                    "SELECT decision FROM manual_labels WHERE jornada_uid=?",
                    (jornada_uid,),
                ).fetchone()
                return row[0] if row else None
        with self._conn.cursor() as cur:
            cur.execute("SELECT decision FROM manual_labels WHERE jornada_uid=%s", (jornada_uid,))
            row = cur.fetchone()
            return row[0] if row else None

    def insert_manual_correction(self, export_id: str, jornada_uid: str, decision: str, note: str = "") -> str:
        cid = str(uuid.uuid4())
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO manual_corrections(id,export_id,jornada_uid,decision,note,imported_at_utc) VALUES(?,?,?,?,?,?)",
                    (cid, export_id, jornada_uid, decision, note, now),
                )
                self._maybe_commit()
            return cid
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO manual_corrections(id,export_id,jornada_uid,decision,note,imported_at_utc) VALUES(%s,%s,%s,%s,%s,%s)",
                (cid, export_id, jornada_uid, decision, note, now),
            )
        return cid

    def insert_export_log(self, export_id: str, range_start_op: str = "", range_end_op: str = "", source_label: str = "", file_name: str = "") -> None:
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        if self.engine == "sqlite":
            with self._lock:
                self._conn.execute(
                    "INSERT INTO export_log(export_id,created_at_utc,range_start_op,range_end_op,source_label,file_name) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(export_id) DO NOTHING",
                    (export_id, now, range_start_op or None, range_end_op or None, source_label or None, file_name or None),
                )
                self._maybe_commit()
            return
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO export_log(export_id,created_at_utc,range_start_op,range_end_op,source_label,file_name) VALUES(%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (export_id) DO NOTHING",
                (export_id, now, range_start_op or None, range_end_op or None, source_label or None, file_name or None),
            )
