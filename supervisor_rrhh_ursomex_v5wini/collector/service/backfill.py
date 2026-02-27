from __future__ import annotations

from datetime import datetime, date, time, timedelta
import time as time_mod
from zoneinfo import ZoneInfo

from collector.utils.timefmt import utc_naive_to_device_time


def day_bounds_utc(yyyy_mm_dd: str, device_tz: ZoneInfo) -> tuple[datetime, datetime]:
    d = date.fromisoformat(yyyy_mm_dd)
    start_local = datetime.combine(d, time(0, 0, 0), tzinfo=device_tz)
    end_local = start_local + timedelta(days=1)
    return (
        start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
    )


def operational_week_bounds_utc(yyyy_mm_dd: str, device_tz: ZoneInfo) -> tuple[datetime, datetime]:
    d = date.fromisoformat(yyyy_mm_dd)
    wd = d.weekday()
    delta_to_wed = (wd - 2) % 7
    start_day = d - timedelta(days=delta_to_wed)
    start_local = datetime.combine(start_day, time(0, 0, 0), tzinfo=device_tz)
    end_local = start_local + timedelta(days=7)
    return (
        start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
    )


def local_window_to_utc(start_day: str, start_hhmm: str, end_day: str, end_hhmm: str, device_tz: ZoneInfo) -> tuple[str, str]:
    sh, sm = [int(x) for x in start_hhmm.split(":")]
    eh, em = [int(x) for x in end_hhmm.split(":")]
    sd = date.fromisoformat(start_day)
    ed = date.fromisoformat(end_day)
    start_local = datetime(sd.year, sd.month, sd.day, sh, sm, 0, tzinfo=device_tz)
    end_local = datetime(ed.year, ed.month, ed.day, eh, em, 0, tzinfo=device_tz)
    start_utc = start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"
    end_utc = end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"
    return start_utc, end_utc


WEEKDAY_MAP = {
    "LUNES": 0,
    "MARTES": 1,
    "MIERCOLES": 2,
    "MIÉRCOLES": 2,
    "JUEVES": 3,
    "VIERNES": 4,
    "SABADO": 5,
    "SÁBADO": 5,
    "DOMINGO": 6,
}


def backfill_range(service, start_dt_utc: datetime, end_dt_utc: datetime, chunk_hours: int = 24) -> int:

    from collector.processing.processor import normalize_event, is_hr_event

    d = service.cfg.device
    page_size = int(service.cfg.pull.get("page_size", 200))

    t0 = time_mod.time()
    pages = 0
    events_total = 0
    raw_inserted = 0
    chunks = 0
    cap_logged = False

    total_inserted = 0
    device_tz = ZoneInfo(service.cfg.device_timezone)
    cursor = start_dt_utc
    while cursor < end_dt_utc:
        chunks += 1
        chunk_end = min(end_dt_utc, cursor + timedelta(hours=chunk_hours))

        start_dev = utc_naive_to_device_time(cursor, device_tz)
        end_dev = utc_naive_to_device_time(chunk_end, device_tz)
        search_id = str(int(time_mod.time() * 1000))

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
                f"Backfill window {start_dev} -> {end_dev} | page {pages} pos={start_pos} received {len(info_list)} events (max_results={page_size}, totalMatches={total_matches}, numOfMatches={num_matches})"
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
                event_time = norm["event_time"] or start_dev
                event_time_utc = norm["event_time_utc"] or cursor.isoformat(timespec="seconds") + "Z"

                service.db.insert_raw(d["ip"], event_time, event_time_utc, payload)
                raw_inserted += 1

                if is_hr_event(payload):
                    inserted = service.db.insert_processed(
                        event_uid=norm["event_uid"],
                        device_ip=d["ip"],
                        event_date=norm["event_date"],
                        event_time=event_time,
                        event_time_utc=event_time_utc,
                        event_type=norm["event_type"],
                        employee_id=norm.get("employee_id"),
                        employee_name=norm.get("employee_name"),
                        payload=norm["payload"],
                    )
                    if inserted:
                        total_inserted += 1

            got = len(info_list)
            if got == 0:
                break

            if isinstance(total_matches, int) and total_matches >= 0:
                if start_pos + got >= total_matches:
                    break

            start_pos += got

        cursor = chunk_end

    elapsed = time_mod.time() - t0
    service._info(
        f"Backfill summary: chunks={chunks} pages={pages} events={events_total} raw_inserted={raw_inserted} rrhh_inserted={total_inserted} "
        f"start_utc={start_dt_utc.isoformat()}Z end_utc={end_dt_utc.isoformat()}Z elapsed_s={elapsed:.2f}"
    )

    service.db.upsert_state("last_backfill_utc", datetime.utcnow().isoformat() + "Z")
    service.db.upsert_state("last_backfill_inserted", str(total_inserted))
    return total_inserted


def backfill_weekday(service, weekday_name: str, days_back: int, chunk_hours: int = 24) -> int:
    wd = WEEKDAY_MAP.get(weekday_name.strip().upper())
    if wd is None:
        raise ValueError("Día inválido")

    device_tz = ZoneInfo(service.cfg.device_timezone)
    now_local = datetime.now(device_tz)
    start_local = (now_local - timedelta(days=days_back)).date()
    end_local = now_local.date()

    total = 0
    d = start_local
    while d <= end_local:
        if d.weekday() == wd:
            start_dt = datetime.combine(d, time(0, 0, 0), tzinfo=device_tz).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
            end_dt = (datetime.combine(d, time(0, 0, 0), tzinfo=device_tz) + timedelta(days=1)).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
            total += backfill_range(service, start_dt, end_dt, chunk_hours=chunk_hours)
        d += timedelta(days=1)

    service.db.upsert_state("last_backfill_weekday", weekday_name.upper())
    service.db.upsert_state("last_backfill_days_back", str(days_back))
    service.db.upsert_state("last_backfill_weekday_inserted", str(total))
    return total
