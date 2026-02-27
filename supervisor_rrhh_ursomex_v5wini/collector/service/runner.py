from __future__ import annotations

import time
from datetime import datetime, timedelta

from collector.client.isapi_client import ISAPIClient
from collector.constants import CHECADOR_LABEL
from collector.processing.processor import normalize_event, is_hr_event
from collector.storage.db import DB
from zoneinfo import ZoneInfo

from collector.utils.timefmt import utc_naive_to_device_time


class CollectorService:
    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.client = ISAPIClient(
            ip=cfg.device["ip"],
            port=cfg.device.get("port", 80),
            user=cfg.device.get("user", "admin"),
            password=cfg.device.get("password", ""),
            endpoints=cfg.device.get("endpoints") or ["/ISAPI/AccessControl/AcsEvent?format=json"],
            device_timezone=cfg.device_timezone,
        )
        self.db = DB(cfg.database.get("engine", "sqlite"), cfg.database.get("sqlite_path", "storage/collector.sqlite"), cfg.database.get("postgres_dsn", ""))
        self._stop = False


        self.db.upsert_state("device_label", CHECADOR_LABEL)
        self.db.upsert_state("device_ip", cfg.device["ip"])

    def _tag(self) -> str:
        return f"[{CHECADOR_LABEL} {self.cfg.device['ip']}]"

    def _info(self, msg: str):
        self.logger.info(f"{self._tag()} {msg}")

    def _warn(self, msg: str):
        self.logger.warning(f"{self._tag()} {msg}")

    def _err(self, msg: str, exc: Exception | None = None):
        if exc is not None:
            self.logger.exception(f"{self._tag()} {msg}", exc_info=exc)
        else:
            self.logger.error(f"{self._tag()} {msg}")

    def stop(self):
        self._stop = True

    def ping(self) -> bool:
        ok = self.client.ping()
        self.db.upsert_state("last_ping_utc", datetime.utcnow().isoformat() + "Z")
        self.db.upsert_state("last_ping_ok", "1" if ok else "0")
        return ok

    def pull_once(self) -> int:

        now = datetime.utcnow().replace(microsecond=0)
        lookback = int(self.cfg.pull.get("lookback_seconds", 300))

        last_cursor = self.db.get_state("last_cursor_utc")
        if last_cursor:
            try:
                start_dt = datetime.fromisoformat(last_cursor.replace("Z", ""))
            except Exception:
                start_dt = now - timedelta(seconds=lookback)
        else:
            start_dt = now - timedelta(seconds=lookback)

        end_dt = now + timedelta(seconds=5)

        start_iso = start_dt.isoformat(timespec="seconds") + "Z"
        end_iso = end_dt.isoformat(timespec="seconds") + "Z"

        device_tz = ZoneInfo(self.cfg.device_timezone)
        start_dev = utc_naive_to_device_time(start_dt, device_tz)
        end_dev = utc_naive_to_device_time(end_dt, device_tz)

        self._info(f"Pull window device {start_dev} -> {end_dev} (UTC {start_iso} -> {end_iso})")

        t0 = time.time()
        pages = 0
        events_total = 0
        raw_inserted = 0
        inserted_processed = 0
        page_size = int(self.cfg.pull.get("page_size", 200))
        start_pos = 0

        latest_seen_utc = start_iso

        search_id = str(int(time.time() * 1000))
        while True:
            data = self.client.pull_acs_events_offset(
                start_time=start_dev,
                end_time=end_dev,
                start_pos=start_pos,
                max_results=page_size,
                search_id=search_id,
                retry_attempts=int(self.cfg.pull.get("retry_attempts", 3)),
                retry_delay=int(self.cfg.pull.get("retry_delay", 3)),
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
                ev = data["AcsEvent"]
                total_matches = ev.get("totalMatches")
                num_matches = ev.get("numOfMatches")

            self._info(
                f"Page {pages} pos={start_pos} received {len(info_list)} events (max_results={page_size}, totalMatches={total_matches}, numOfMatches={num_matches})"
            )

            if not info_list:
                break

            for ev in info_list:
                payload = dict(ev)
                payload.setdefault("device_ip", self.cfg.device["ip"])

                norm = normalize_event(payload, device_ip=self.cfg.device["ip"])
                event_time = norm["event_time"] or start_iso
                event_time_utc = norm["event_time_utc"] or start_iso

                payload.setdefault("event_time", event_time)
                payload.setdefault("event_time_utc", event_time_utc)

                self.db.insert_raw(self.cfg.device["ip"], event_time, event_time_utc, payload)
                raw_inserted += 1

                if is_hr_event(payload):
                    if self.db.insert_processed(
                        event_uid=norm["event_uid"],
                        device_ip=self.cfg.device["ip"],
                        event_date=norm.get("event_date") or "",
                        event_time=event_time,
                        event_time_utc=event_time_utc,
                        event_type=norm.get("event_type") or "",
                        employee_id=norm.get("employee_id"),
                        employee_name=norm.get("employee_name"),
                        payload=norm["payload"],
                    ):
                        inserted_processed += 1


                if event_time_utc and event_time_utc > latest_seen_utc:
                    latest_seen_utc = event_time_utc


            got = len(info_list)
            if got == 0:
                break

            if isinstance(total_matches, int) and total_matches >= 0:
                if start_pos + got >= total_matches:
                    break

            start_pos += got

        self.db.upsert_state("last_pull_utc", datetime.utcnow().isoformat() + "Z")
        self.db.upsert_state("last_pull_inserted", str(inserted_processed))


        self.db.upsert_state("last_cursor_utc", latest_seen_utc)

        elapsed = time.time() - t0
        self._info(
            f"Pull summary: pages={pages} events={events_total} raw_inserted={raw_inserted} rrhh_inserted={inserted_processed} "
            f"cursor={latest_seen_utc} elapsed_s={elapsed:.2f}"
        )


        try:
            self.db.prune_old(int(self.cfg.storage.get("keeprawdays", 90)), int(self.cfg.storage.get("keepprocesseddays", 365)))
            self.db.sqlite_backup_if_needed(self.cfg.storage.get("backup_dir", "storage/backups"), int(self.cfg.storage.get("maxdbsize_mb", 1024)))
        except Exception as e:
            self._warn(f"Maintenance error: {e}")

        return inserted_processed

    def run_forever(self, realtime: bool = False):
        self._stop = False
        interval = int(self.cfg.pull.get("realtimepullinterval" if realtime else "normalpullinterval", 60))
        mode = "realtime" if realtime else "normal"
        self._info(f"Starting loop mode={mode} interval={interval}s")

        while not self._stop:
            try:
                self.ping()
                self.pull_once()
            except Exception as e:
                self.db.upsert_state("last_error", str(e))
                self.db.upsert_state("last_error_utc", datetime.utcnow().isoformat() + "Z")
                self._err("Loop error", e)
            time.sleep(max(1, interval))

        self._info("Stopped loop")
