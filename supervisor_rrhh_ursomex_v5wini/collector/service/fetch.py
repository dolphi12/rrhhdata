from __future__ import annotations

from datetime import datetime, timedelta
import time
from typing import Any, Dict, List, Tuple

from collector.processing.processor import normalize_event, is_hr_event
from zoneinfo import ZoneInfo

from collector.utils.timefmt import utc_naive_to_device_time


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds") + "Z"


def fetch_from_device_range(
    service,
    start_dt_utc: datetime,
    end_dt_utc: datetime,
    chunk_hours: int = 6,
    persist_to_db: bool = True,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    d = service.cfg.device
    page_size = int(service.cfg.pull.get("page_size", 200))

    t0 = time.time()
    pages = 0
    events_total = 0
    raw_inserted = 0
    rrhh_rows = 0
    chunks = 0
    cap_logged = False

    audit_all: List[Dict[str, Any]] = []
    rrhh_flat: List[Dict[str, Any]] = []

    device_tz = ZoneInfo(service.cfg.device_timezone)
    cursor = start_dt_utc
    while cursor < end_dt_utc:
        chunks += 1
        chunk_end = min(end_dt_utc, cursor + timedelta(hours=chunk_hours))
        start_iso = _iso(cursor)
        end_iso = _iso(chunk_end)
        start_dev = utc_naive_to_device_time(cursor, device_tz)
        end_dev = utc_naive_to_device_time(chunk_end, device_tz)
        search_id = str(int(time.time() * 1000))

        start_pos = 0
        while True:
            data = service.client.pull_acs_events_offset(
                start_time=start_dev,
                end_time=end_dev,
                start_pos=start_pos,
                max_results=page_size,
                search_id=search_id,
                retry_attempts=int(service.cfg.pull.get("retry_attempts", 3)),
                retry_delay=int(service.cfg.pull.get("retry_delay", 3)),
            )

            info_list = []
            if isinstance(data, dict):
                if "AcsEvent" in data and isinstance(data["AcsEvent"], dict):
                    info_list = data["AcsEvent"].get("InfoList", []) or []
                else:
                    info_list = data.get("InfoList", []) or []


            pages += 1
            events_total += len(info_list)
            total_matches = None
            num_matches = None
            if isinstance(data, dict) and isinstance(data.get("AcsEvent"), dict):
                evm = data["AcsEvent"]
                total_matches = evm.get("totalMatches")
                num_matches = evm.get("numOfMatches")

            service._info(
                f"Export-from-checador window {start_dev} -> {end_dev} (UTC {start_iso} -> {end_iso}) | page {pages} pos={start_pos} received {len(info_list)} events (max_results={page_size}, totalMatches={total_matches}, numOfMatches={num_matches})"
            )


            if (not cap_logged and start_pos == 0 and isinstance(total_matches, int)
                and total_matches > len(info_list) and 0 < len(info_list) < page_size):
                cap_logged = True
                cap = len(info_list)
                try:
                    service.db.upsert_state("device_page_cap", str(cap))
                except Exception:
                    pass
                service._warn(
                    f"Firmware cap detectado: device_limit={cap} por llamada (max_results solicitado={page_size}). "
                    "Se paginará por offset hasta terminar."
                )


            if not info_list:
                break

            for ev in info_list:
                payload = dict(ev)
                payload.setdefault("device_ip", d["ip"])
                norm = normalize_event(payload, device_ip=d["ip"])

                event_time = norm["event_time"] or start_iso
                event_time_utc = norm["event_time_utc"] or start_iso

                payload.setdefault("event_time", event_time)
                payload.setdefault("event_time_utc", event_time_utc)

                audit_all.append(payload)

                if persist_to_db:
                    service.db.insert_raw(d["ip"], event_time, event_time_utc, payload)
                    raw_inserted += 1

                if is_hr_event(payload):
                    row = {
                        "event_uid": norm["event_uid"],
                        "device_ip": norm.get("device_ip") or d["ip"],
                        "event_date": norm.get("event_date") or "",
                        "event_time": event_time,
                        "event_time_utc": event_time_utc,
                        "employee_id": norm.get("employee_id"),
                        "employee_name": norm.get("employee_name"),
                        "event_type": norm.get("event_type") or "",
                        "verify_mode": norm.get("verify_mode"),
                        "result_bucket": norm.get("result_bucket"),
                        "attendance_status": norm.get("attendance_status"),
                        "label": norm.get("label"),
                        "picture_url": norm.get("picture_url"),
                    }
                    rrhh_flat.append(row)
                    rrhh_rows += 1

                    if persist_to_db:
                        service.db.insert_processed(
                            event_uid=norm["event_uid"],
                            device_ip=d["ip"],
                            event_date=norm.get("event_date") or "",
                            event_time=event_time,
                            event_time_utc=event_time_utc,
                            event_type=norm.get("event_type") or "",
                            employee_id=norm.get("employee_id"),
                            employee_name=norm.get("employee_name"),
                            payload=norm["payload"],
                        )

            got = len(info_list)
            if got == 0:
                break
            if isinstance(total_matches, int) and total_matches >= 0:
                if start_pos + got >= total_matches:
                    break
            start_pos += got

        cursor = chunk_end

    elapsed = time.time() - t0
    service._info(
        f"Export-from-checador summary: chunks={chunks} pages={pages} events={events_total} raw_saved={raw_inserted if persist_to_db else 0} rrhh_rows={rrhh_rows} "
        f"persist_to_db={persist_to_db} start={_iso(start_dt_utc)} end={_iso(end_dt_utc)} elapsed_s={elapsed:.2f}"
    )

    return audit_all, rrhh_flat
