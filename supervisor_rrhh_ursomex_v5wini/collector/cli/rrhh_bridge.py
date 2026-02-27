from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from collector.config import load_config

# Paquete del supervisor integrado
from rrhh_supervisor.cli import run_with_config_dict


def _norm_path(p: str) -> str:
    # Normaliza a forward slashes (Windows friendly, URI friendly)
    return str(p).replace('\\', '/')


def build_rrhh_config_dict(collector_config_path: str) -> Dict[str, Any]:
    cfg = load_config(collector_config_path)

    # DB del collector (fuente de datos)
    db = cfg.database or {}
    engine = (db.get("engine") or "sqlite").lower().strip()
    sqlite_path = db.get("sqlite_path") or "storage/collector.sqlite"

    # Sección rrhh opcional dentro del config del collector (no está en la dataclass Config)
    raw_cfg: Dict[str, Any] = {}
    try:
        import json
        raw_cfg = json.loads(Path(collector_config_path).read_text(encoding='utf-8')).get('rrhh', {}) or {}
    except Exception:
        raw_cfg = {}


    # Defaults (si no existen en config.json)
    data_dir = (cfg.storage or {}).get("data_dir", "storage")
    rrhh_store_path = raw_cfg.get("store_sqlite_path") or f"{data_dir}/rrhh_store.sqlite3"
    roster_csv_path = raw_cfg.get("roster_csv_path") or f"{data_dir}/roster_empleados.csv"
    out_dir = raw_cfg.get("out_dir") or f"{data_dir}/rrhh_exports"

    # Operation
    shift_cutoff = (cfg.operation or {}).get("shift_cutoff", "03:00")

    # Presence/analytics/predictions defaults
    presence = raw_cfg.get("presence", {}) if isinstance(raw_cfg.get("presence", {}), dict) else {}
    analytics = raw_cfg.get("analytics", {}) if isinstance(raw_cfg.get("analytics", {}), dict) else {}
    predictions = raw_cfg.get("predictions", {}) if isinstance(raw_cfg.get("predictions", {}), dict) else {}
    roster = raw_cfg.get("roster", {}) if isinstance(raw_cfg.get("roster", {}), dict) else {}

    merged: Dict[str, Any] = {
        "local_tz": cfg.device_timezone,
        "operation": {"shift_cutoff_hhmm": shift_cutoff},
        "collector_database": {
            "engine": engine,
            "sqlite_path": _norm_path(sqlite_path),
            "postgres_dsn": db.get("postgres_dsn", "") or "",
        },
        "rrhh_store": {
            "engine": "sqlite",
            "sqlite_path": _norm_path(rrhh_store_path),
            "postgres_dsn": "",
        },
        "mode": {"collector_read_only": True},
        "roster": {
            "mode": roster.get("mode", "active_last_days"),
            "active_last_days": int(roster.get("active_last_days", 90)),
            "csv_path": _norm_path(roster.get("csv_path", roster_csv_path)),
            "id_min_width": int(roster.get("id_min_width", 1)),
            "default_active": bool(roster.get("default_active", True)),
            "only_active": bool(roster.get("only_active", True)),
        },
        "presence": {
            "stale_after_minutes": int(presence.get("stale_after_minutes", 720)),
            "pause_max_minutes": int(presence.get("pause_max_minutes", 240)),
        },
        "analytics": {
            "windows_days": analytics.get("windows_days", [30, 60, 90]),
            "min_jornadas_for_profile": int(analytics.get("min_jornadas_for_profile", 3)),
        },
        "predictions": {
            "entry_window_minutes": int(predictions.get("entry_window_minutes", 75)),
            "confidence_min_samples": int(predictions.get("confidence_min_samples", 5)),
            "window_days": int(predictions.get("window_days", 60)),
        },
        "exports": {"out_dir": _norm_path(out_dir)},
    }

    # Asegura folders
    Path(os.path.dirname(merged["rrhh_store"]["sqlite_path"]) or ".").mkdir(parents=True, exist_ok=True)
    Path(merged["exports"]["out_dir"]).mkdir(parents=True, exist_ok=True)

    return merged


def run_rrhh_supervisor(collector_config_path: str):
    data = build_rrhh_config_dict(collector_config_path)
    # Se escribe config temporal dentro del out_dir para evitar que quede en raíz
    tmp_path = os.path.join((data.get("exports", {}) or {}).get("out_dir", "storage/rrhh_exports"), ".rrhh_runtime_config.json")
    run_with_config_dict(data, _tmp_path=tmp_path)
