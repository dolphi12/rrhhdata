from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Dict, Tuple

from collector.processing.processor import is_hr_event, normalize_event


def import_jsonl_file(
    service,
    jsonl_path: str,
    device_ip_fallback: str,
    persist_to_db: bool = True,
) -> Tuple[int, int, int]:


    p = Path(jsonl_path)
    if not p.exists():
        raise FileNotFoundError(f"No existe: {jsonl_path}")

    raw_n = 0
    rrhh_n = 0
    err_n = 0

    tx = service.db.transaction() if getattr(service.db, 'transaction', None) and service.db.engine == 'sqlite' else nullcontext()

    with tx:
        with p.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload: Dict[str, Any] = json.loads(line)
                    if not isinstance(payload, dict):
                        err_n += 1
                        continue

                    norm = normalize_event(payload)
                    event_time = norm.get("event_time") or payload.get("event_time") or ""
                    event_time_utc = norm.get("event_time_utc") or payload.get("event_time_utc") or ""

                    device_ip = str(payload.get("device_ip") or norm.get("device_ip") or device_ip_fallback)
                    payload.setdefault("device_ip", device_ip)
                    if event_time:
                        payload.setdefault("event_time", event_time)
                    if event_time_utc:
                        payload.setdefault("event_time_utc", event_time_utc)

                    if persist_to_db:
                        service.db.insert_raw(device_ip, event_time or "0000-00-00T00:00:00Z", event_time_utc or "0000-00-00T00:00:00Z", payload)
                        raw_n += 1

                    if is_hr_event(payload):
                        inserted = True
                        if persist_to_db:
                            inserted = service.db.insert_processed(
                                event_uid=norm["event_uid"],
                                device_ip=device_ip,
                                event_date=norm.get("event_date") or "0000-00-00",
                                event_time=event_time or "0000-00-00T00:00:00Z",
                                event_time_utc=event_time_utc or "0000-00-00T00:00:00Z",
                                event_type=norm.get("event_type") or "unknown",
                                employee_id=norm.get("employee_id"),
                                employee_name=norm.get("employee_name"),
                                payload=norm.get("payload") or payload,
                            )
                        if inserted:
                            rrhh_n += 1

                except Exception:
                    err_n += 1

    return raw_n, rrhh_n, err_n
