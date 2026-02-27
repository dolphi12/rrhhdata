from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


@dataclass(frozen=True)
class AppConfig:
    raw: Dict[str, Any]

    @property
    def db_engine(self) -> str:
        return str(self.raw.get("database", {}).get("engine", "sqlite")).lower().strip()

    @property
    def sqlite_path(self) -> str:
        return str(self.raw.get("database", {}).get("sqlite_path", "data/collector.sqlite3"))

    @property
    def postgres_dsn(self) -> str:
        return str(self.raw.get("database", {}).get("postgres_dsn", ""))


    @property
    def collector_db_engine(self) -> str:
        v = self.raw.get("collector_database", {})
        if isinstance(v, dict) and v.get("engine"):
            return str(v.get("engine")).lower().strip()
        return self.db_engine

    @property
    def collector_sqlite_path(self) -> str:
        v = self.raw.get("collector_database", {})
        if isinstance(v, dict) and v.get("sqlite_path"):
            return str(v.get("sqlite_path"))
        return self.sqlite_path

    @property
    def collector_postgres_dsn(self) -> str:
        v = self.raw.get("collector_database", {})
        if isinstance(v, dict) and v.get("postgres_dsn") is not None:
            return str(v.get("postgres_dsn") or "")
        return self.postgres_dsn

    @property
    def store_db_engine(self) -> str:
        v = self.raw.get("rrhh_store", {})
        return str((v or {}).get("engine", "sqlite")).lower().strip()

    @property
    def store_sqlite_path(self) -> str:
        v = self.raw.get("rrhh_store", {})
        return str((v or {}).get("sqlite_path", "data/rrhh_store.sqlite3"))

    @property
    def store_postgres_dsn(self) -> str:
        v = self.raw.get("rrhh_store", {})
        return str((v or {}).get("postgres_dsn", ""))

    @property
    def collector_read_only(self) -> bool:
        v = self.raw.get("mode", {})
        if isinstance(v, dict) and "collector_read_only" in v:
            return bool(v.get("collector_read_only"))
        return True
    @property
    def local_tz(self) -> str:
        return str(self.raw.get("local_tz", "America/Mexico_City"))

    @property
    def shift_cutoff_hhmm(self) -> str:
        return str(self.raw.get("operation", {}).get("shift_cutoff_hhmm", "03:00"))

    @property
    def roster_mode(self) -> str:
        return str(self.raw.get("roster", {}).get("mode", "active_last_days")).lower().strip()

    @property
    def roster_active_last_days(self) -> int:
        return int(self.raw.get("roster", {}).get("active_last_days", 30))

    @property
    def roster_csv_path(self) -> str:
        return str(self.raw.get("roster", {}).get("csv_path", ""))


    @property
    def roster_id_min_width(self) -> int:
        return int(self.raw.get("roster", {}).get("id_min_width", 0))

    @property
    def roster_default_active(self) -> int:
        return int(self.raw.get("roster", {}).get("default_active", 1))

    @property
    def roster_only_active(self) -> bool:
        return bool(self.raw.get("roster", {}).get("only_active", True))

    @property
    def stale_after_minutes(self) -> int:
        return int(self.raw.get("presence", {}).get("stale_after_minutes", 720))

    @property
    def pause_max_minutes(self) -> int:
        return int(self.raw.get("presence", {}).get("pause_max_minutes", 240))

    @property
    def analytics_windows_days(self) -> List[int]:
        arr = self.raw.get("analytics", {}).get("windows_days", [30, 60, 90])
        return [int(x) for x in arr]

    @property
    def min_jornadas_for_profile(self) -> int:
        return int(self.raw.get("analytics", {}).get("min_jornadas_for_profile", 3))

    @property
    def entry_window_minutes(self) -> int:
        return int(self.raw.get("predictions", {}).get("entry_window_minutes", 75))

    @property
    def confidence_min_samples(self) -> int:
        return int(self.raw.get("predictions", {}).get("confidence_min_samples", 5))


    @property
    def predictor_window_days(self) -> int:
        return int(self.raw.get("predictions", {}).get("window_days", 60))

    @property
    def out_dir(self) -> str:
        return str(self.raw.get("exports", {}).get("out_dir", "exports"))




def load_config_from_dict(data: Dict[str, Any]) -> AppConfig:
    """Crea AppConfig desde un dict (para integración con el Collector)."""
    if not isinstance(data, dict):
        raise TypeError("data debe ser dict")
    return AppConfig(raw=data)
def load_config(path: str) -> AppConfig:
    p = Path(path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("config inválido")
    return AppConfig(raw=raw)
