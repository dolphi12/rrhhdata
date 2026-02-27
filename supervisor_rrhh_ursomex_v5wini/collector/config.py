from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from collector.constants import DEFAULT_TIMEZONE


@dataclass
class Config:
    device: Dict[str, Any]
    device_timezone: str
    pull: Dict[str, Any]
    storage: Dict[str, Any]
    export: Dict[str, Any]
    operation: Dict[str, Any]
    database: Dict[str, Any]


def _deep_merge(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            dst[k] = _deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst


def default_config() -> Dict[str, Any]:
    return {
        'device_timezone': DEFAULT_TIMEZONE,
        'device': {
            'ip': '192.168.1.227',
            'port': 80,
            'user': 'admin',
            'password': 'admin',
            'endpoints': [
                '/ISAPI/AccessControl/AcsEvent?format=json',
            ],
        },
        'pull': {
            'normalpullinterval': 60,
            'realtimepullinterval': 10,
            'retry_attempts': 3,
            'retry_delay': 3,
            'page_size': 200,
            'autoadjustlookahead': True,
            'lookback_seconds': 300,
        },
        'storage': {
            'data_dir': 'storage',
            'backup_dir': 'storage/backups',
            'keeprawdays': 90,
            'keepprocesseddays': 365,
            'maxdbsize_mb': 1024,
        },
        'export': {
            'generatedailyexcel': True,
            'generateweeklyexcel': True,
            'generatejsonexport': True,
            'excel_template': '',
            'procesador_dir': 'procesador_dir',
        },
        'operation': {
            'shift_cutoff': '03:00',
            'expectedmaxexit_hhmm': '23:59',
            'week_start': 'WEDNESDAY',
        },
        'database': {
            'engine': 'sqlite',
            'sqlite_path': 'storage/collector.sqlite',
            'postgres_dsn': '',
        },
    }


def load_config(path: str | Path) -> Config:
    path = Path(path)
    cfg = default_config()

    if path.exists():
        data = json.loads(path.read_text(encoding='utf-8'))
        cfg = _deep_merge(cfg, data)


    data_dir = Path(cfg['storage']['data_dir'])
    data_dir.mkdir(parents=True, exist_ok=True)
    Path(cfg['storage']['backup_dir']).mkdir(parents=True, exist_ok=True)


    cfg['device']['port'] = int(cfg['device'].get('port', 80))
    cfg['pull']['normalpullinterval'] = int(cfg['pull'].get('normalpullinterval', 60))
    cfg['pull']['realtimepullinterval'] = int(cfg['pull'].get('realtimepullinterval', 10))
    cfg['pull']['retry_attempts'] = int(cfg['pull'].get('retry_attempts', 3))
    cfg['pull']['retry_delay'] = int(cfg['pull'].get('retry_delay', 3))
    cfg['pull']['page_size'] = int(cfg['pull'].get('page_size', 200))
    cfg['pull']['lookback_seconds'] = int(cfg['pull'].get('lookback_seconds', 300))
    cfg['storage']['keeprawdays'] = int(cfg['storage'].get('keeprawdays', 90))
    cfg['storage']['keepprocesseddays'] = int(cfg['storage'].get('keepprocesseddays', 365))
    cfg['storage']['maxdbsize_mb'] = int(cfg['storage'].get('maxdbsize_mb', 1024))


    cfg['device_timezone'] = str(cfg.get('device_timezone') or DEFAULT_TIMEZONE)

    return Config(
        device=cfg['device'],
        device_timezone=cfg['device_timezone'],
        pull=cfg['pull'],
        storage=cfg['storage'],
        export=cfg['export'],
        operation=cfg['operation'],
        database=cfg['database'],
    )
