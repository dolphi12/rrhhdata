from __future__ import annotations

import os
import json
import uuid
from datetime import datetime, timedelta
from threading import Thread
from typing import Optional

from zoneinfo import ZoneInfo

from collector.config import load_config
from collector.constants import CHECADOR_LABEL
from collector.export.excel_exporter import export_excel_jornadas_summary
from collector.log import setup_logger
from collector.service.import_corrections_excel import import_manual_corrections_excel
from collector.service.backfill import (
    WEEKDAY_MAP,
    backfill_range,
    backfill_weekday,
    day_bounds_utc,
    local_window_to_utc,
    operational_week_bounds_utc,
)
from collector.service.fetch import fetch_from_device_range
from collector.service.import_jsonl import import_jsonl_file
from collector.service.runner import CollectorService
from collector.service.jornadas_indexer import (
    ensure_jornadas_indexed_until,
    jornadas_to_export_rows,
)

from collector.cli.rrhh_bridge import run_rrhh_supervisor


DEFAULT_CONFIG_PATH = "config.json"
def _device_tz(cfg) -> ZoneInfo:
    return ZoneInfo(cfg.device_timezone)


def _input(prompt: str) -> str:
    return input(prompt).strip()


def _date_only(s: str) -> str:

    parts = (s or "").strip().split()
    return parts[0] if parts else ""


def _press_enter():
    input("\nEnter para continuar...")


def local_today_iso(cfg) -> str:
    return datetime.now(_device_tz(cfg)).date().isoformat()


def choose_export_source() -> str:
    print("\nFuente del export:")
    print("1) DB (lo ya guardado)")
    print(f"2) {CHECADOR_LABEL} (Hikvision) — extrae, GUARDA y exporta")
    s = _input("Opción: ")
    return "db" if s == "1" else "checador"


def source_label(source: str) -> str:
    return "DB" if source == "db" else "CHECADOR_ASISTENCIAS"


def _rrhh_flat_from_db(svc: CollectorService, day: str):
    rrhh = svc.db.get_processed_by_date(day)
    rrhh_flat = []
    for e in rrhh:
        p = e.get("payload") or {}
        rrhh_flat.append(
            {
                "event_uid": e.get("event_uid"),
                "device_ip": e.get("device_ip"),
                "event_date": e.get("event_date"),
                "event_time": e.get("event_time"),
                "event_time_utc": e.get("event_time_utc"),
                "employee_id": e.get("employee_id"),
                "employee_name": e.get("employee_name"),
                "event_type": e.get("event_type"),
                "verify_mode": p.get("verify_mode") or p.get("currentVerifyMode"),
                "result_bucket": p.get("result_bucket"),
                "attendance_status": p.get("attendance_status") or p.get("attendanceStatus"),
                "label": p.get("label"),
                "picture_url": p.get("picture_url") or p.get("pictureURL") or p.get("pictureUrl"),
            }
        )
    return rrhh_flat


def _datasets_day(svc: CollectorService, source: str, day: str):
    if source == "db":
        audit = svc.db.get_raw_by_date(day)
        rrhh_flat = _rrhh_flat_from_db(svc, day)
        return audit, rrhh_flat

    tz = _device_tz(svc.cfg)
    start_dt, end_dt = day_bounds_utc(day, tz)
    audit, rrhh_flat = fetch_from_device_range(svc, start_dt, end_dt, chunk_hours=6, persist_to_db=True)
    return audit, rrhh_flat


def _datasets_window(svc: CollectorService, source: str, s1: str, s2: str):
    start_day, start_hhmm = s1.split()
    end_day, end_hhmm = s2.split()
    tz = _device_tz(svc.cfg)
    start_utc, end_utc = local_window_to_utc(start_day, start_hhmm, end_day, end_hhmm, tz)

    if source == "db":
        rrhh = svc.db.get_processed_by_utc_range(start_utc, end_utc)
        audit = svc.db.get_raw_by_utc_range(start_utc, end_utc)
        rrhh_flat = []
        for e in rrhh:
            p = e.get("payload") or {}
            rrhh_flat.append(
                {
                    "event_uid": e.get("event_uid"),
                    "device_ip": e.get("device_ip"),
                    "event_date": e.get("event_date"),
                    "event_time": e.get("event_time"),
                    "event_time_utc": e.get("event_time_utc"),
                    "employee_id": e.get("employee_id"),
                    "employee_name": e.get("employee_name"),
                    "event_type": e.get("event_type"),
                    "verify_mode": p.get("verify_mode") or p.get("currentVerifyMode"),
                    "result_bucket": p.get("result_bucket"),
                    "attendance_status": p.get("attendance_status") or p.get("attendanceStatus"),
                    "label": p.get("label"),
                    "picture_url": p.get("picture_url") or p.get("pictureURL") or p.get("pictureUrl"),
                }
            )
        return audit, rrhh_flat, start_utc, end_utc

    start_dt = datetime.fromisoformat(start_utc.replace("Z", ""))
    end_dt = datetime.fromisoformat(end_utc.replace("Z", ""))
    audit, rrhh_flat = fetch_from_device_range(svc, start_dt, end_dt, chunk_hours=6, persist_to_db=True)
    return audit, rrhh_flat, start_utc, end_utc


def run_dashboard(cfg_path: str | None = None):
    cfg_path = cfg_path or os.environ.get("ISAPI_CONFIG", DEFAULT_CONFIG_PATH)
    cfg = load_config(cfg_path)

    log_dir = os.path.join(cfg.storage["data_dir"], "logs")
    logger = setup_logger(log_dir)

    svc = CollectorService(cfg, logger)
    bg_thread: Optional[Thread] = None

    def start_bg(realtime: bool):
        nonlocal bg_thread
        if bg_thread and bg_thread.is_alive():
            print("Servicio ya está corriendo.")
            return
        bg_thread = Thread(target=svc.run_forever, kwargs={"realtime": realtime}, daemon=True)
        bg_thread.start()
        print(f"Servicio iniciado en segundo plano (modo={'realtime' if realtime else 'normal'}).")

    def stop_bg():
        nonlocal bg_thread
        svc.stop()
        if bg_thread:
            bg_thread.join(timeout=2)
        print("Servicio detenido.")

    def show_status():
        today = local_today_iso(cfg)
        ip = cfg.device.get("ip")

        last_pull = svc.db.get_state("last_pull_utc")
        last_ins = svc.db.get_state("last_pull_inserted")
        last_ping = svc.db.get_state("last_ping_utc")
        last_ping_ok = svc.db.get_state("last_ping_ok")
        cap = svc.db.get_state("device_page_cap")

        raw_total = svc.db.count_raw()
        proc_total = svc.db.count_processed()

        rrhh_today_n = svc.db.count_processed_by_date(today)
        audit_today_n = svc.db.count_raw_by_date(today)

        vm_counts = svc.db.get_verify_mode_counts_raw_by_date(today)
        total = vm_counts.get("total", 0)
        invalid = vm_counts.get("invalid", 0)
        valid = vm_counts.get("valid", 0)
        ratio = f"{invalid}/{valid}" if valid > 0 else f"{invalid}/0"
        invalid_pct = (invalid / total * 100.0) if total else 0.0

        last_raw_evt = svc.db.get_last_raw_event_time()
        last_rrhh_evt = svc.db.get_last_processed_event_time()

        print("\n--- Estado (Operación 24/7) ---")
        print(f"{CHECADOR_LABEL} IP: {ip}")
        print(f"Hoy ({cfg.device_timezone}): {today}")
        print(f"RRHH hoy (procesados): {rrhh_today_n}")
        print(f"Auditoría hoy (raw): {audit_today_n}")
        print(f"verify_mode invalid/valid: {ratio} | invalid%={invalid_pct:.1f}%")

        print("\n--- Señal de vida del checador ---")
        print(f"Último evento RAW (event_time): {last_raw_evt}")
        print(f"Último evento RRHH (event_time): {last_rrhh_evt}")

        print("\n--- Conectividad / Pull ---")
        print(f"Último ping: {last_ping} ok={last_ping_ok}")
        print(f"Último pull: {last_pull} insertados_RRHH={last_ins}")

        print("\n--- Paginación / Firmware ---")
        print(f"Cap detectado por llamada: {cap or '(no detectado)'}")

        print("\n--- Base de datos ---")
        print(f"DB raw total: {raw_total} | processed total: {proc_total}")

        # Model/pattern persistence sanity check
        try:
            st_n = svc.db.count_employee_jornada_states()
            audit_n = svc.db.count_model_audit()
            gp = json.loads(svc.db.get_state("model_global_profile") or "{}") or {}
            gp_n = float(gp.get("n") or 0.0)
            print(
                f"Patrones/modelo guardados: {st_n} empleados | audit decisiones: {audit_n} | global_n~{gp_n:.2f}"
            )
            try:
                s2 = json.loads(svc.db.get_state("model_seasonality_v2_state") or "{}") or {}
                if isinstance(s2, dict) and bool(s2.get("enabled", False)):
                    pm = bool(s2.get("peak_mode_current", False))
                    ioi = float(s2.get("ioi_current") or 0.0)
                    mx = str(s2.get("max_op_date") or "")
                    print(f"Seasonality v2: peak_mode={pm} | IOI={ioi:.2f} | max_op_date={mx}")
            except Exception:
                pass

            try:
                c2 = json.loads(svc.db.get_state("model_cluster_v2_state") or "{}") or {}
                if isinstance(c2, dict) and bool(c2.get("enabled", False)):
                    k = int(c2.get("k") or 0)
                    wd = int(c2.get("window_days") or 0)
                    ld = str(c2.get("last_op_date") or "")
                    ce = int(c2.get("clustered_employees") or 0)
                    labels = c2.get("labels") or []
                    labels_s = ",".join([str(x) for x in labels]) if isinstance(labels, list) else ""
                    mode = str(c2.get("refresh_mode") or "")
                    wk = str(c2.get("last_refresh_week_start") or "")
                    mx = str(c2.get("max_op_date_seen") or "")
                    mode_s = mode or "interval"
                    print(
                        f"Cluster v2: mode={mode_s} | k={k} | window={wd}d | wk_start={wk} | end_op={ld} | max_op={mx} | clustered={ce} | labels={labels_s}"
                    )

            except Exception:
                pass
            try:
                wa = svc.db.get_last_weekly_audit()
                if isinstance(wa, dict) and wa:
                    wk_s = str(wa.get("week_start_op") or "")
                    wk_e = str(wa.get("week_end_op") or "")
                    pm = bool(wa.get("peak_mode", False))
                    ioi_m = float(wa.get("ioi_mean") or 0.0)
                    ioi_e = float(wa.get("ioi_end") or 0.0)
                    jpd = int(wa.get("jpd_sum") or 0)
                    d1 = float(wa.get("d1_rate_mean") or 0.0)
                    ck = int(wa.get("cluster_k") or 0)
                    cc = wa.get("cluster_counts") or {}
                    cc_s = ",".join([f"{k}:{cc[k]}" for k in sorted(cc.keys())]) if isinstance(cc, dict) else ""
                    print(
                        f"Snapshot semanal: {wk_s}→{wk_e} | peak={pm} | IOI_mean={ioi_m:.2f} | IOI_end={ioi_e:.2f} | jpd={jpd} | d1%~{(d1*100):.1f}% | k={ck} | clusters={cc_s}"
                    )
            except Exception:
                pass



        except Exception:
            pass

    def menu_backfill():
        while True:
            print("\n=== Backfill (histórico del checador) ===")
            print("1) X días hacia atrás")
            print("2) Fecha exacta (un día) YYYY-MM-DD")
            print("3) Rango por fechas YYYY-MM-DD a YYYY-MM-DD")
            print("4) Día de la semana (todos los LUNES/MARTES/...)")
            print("5) Semana operativa (Mié→Mar) que contiene una fecha")
            print("6) Ventana por horas (YYYY-MM-DD HH:MM -> YYYY-MM-DD HH:MM)")
            print("7) Importar JSONL a DB (para pruebas / re-proceso)")
            print("8) Importar JSONL y Exportar ahora (elige DB o Checador Asistencias)")
            print("0) Volver")

            op = _input("Opción: ")
            if op == "0":
                return

            try:
                if op == "1":
                    days = int(_input("¿Cuántos días hacia atrás? (ej 30, 90): "))
                    chunk = int(_input("Chunk horas por bloque (recomendado 24): ") or "24")
                    end_dt = datetime.utcnow().replace(microsecond=0)
                    start_dt = end_dt - timedelta(days=days)
                    n = backfill_range(svc, start_dt, end_dt, chunk_hours=chunk)
                    print(f"Backfill OK. RRHH insertados (dedupe): {n}")

                elif op == "2":
                    day = _input("Fecha (YYYY-MM-DD): ")
                    chunk = int(_input("Chunk horas por bloque (recomendado 24): ") or "24")
                    tz = _device_tz(cfg)
                    start_dt, end_dt = day_bounds_utc(day, tz)
                    n = backfill_range(svc, start_dt, end_dt, chunk_hours=chunk)
                    print(f"Backfill OK ({day}). RRHH insertados: {n}")

                elif op == "3":
                    left_raw = _input("Inicio (YYYY-MM-DD): ")
                    right_raw = _input("Fin (YYYY-MM-DD): ")
                    left = _date_only(left_raw)
                    right = _date_only(right_raw)
                    if (left_raw and left_raw != left) or (right_raw and right_raw != right):
                        print("Nota: En 'Rango por fechas' solo se usa la FECHA. Si quieres horas usa la opción 6 (Ventana por horas).")
                    chunk = int(_input("Chunk horas por bloque (recomendado 24): ") or "24")
                    tz = _device_tz(cfg)
                    start_dt, _ = day_bounds_utc(left, tz)
                    _, end_dt = day_bounds_utc(right, tz)
                    n = backfill_range(svc, start_dt, end_dt, chunk_hours=chunk)
                    print(f"Backfill OK ({left} a {right}). RRHH insertados: {n}")

                elif op == "4":
                    weekday = _input("Día (LUNES/MARTES/MIERCOLES/JUEVES/VIERNES/SABADO/DOMINGO): ").upper()
                    days_back = int(_input("¿Cuántos días hacia atrás? (ej 30, 90): "))
                    chunk = int(_input("Chunk horas por bloque (recomendado 24): ") or "24")
                    n = backfill_weekday(svc, weekday, days_back, chunk_hours=chunk)
                    print(f"Backfill OK ({weekday}). RRHH insertados: {n}")

                elif op == "5":
                    day = _input("Fecha dentro de la semana (YYYY-MM-DD): ")
                    chunk = int(_input("Chunk horas por bloque (recomendado 24): ") or "24")
                    tz = _device_tz(cfg)
                    start_dt, end_dt = operational_week_bounds_utc(day, tz)
                    n = backfill_range(svc, start_dt, end_dt, chunk_hours=chunk)
                    print(f"Backfill OK. RRHH insertados: {n}")

                elif op == "6":
                    print("Formato: YYYY-MM-DD HH:MM")
                    s1 = _input("Inicio: ")
                    s2 = _input("Fin: ")
                    chunk = int(_input("Chunk horas por bloque (recomendado 6): ") or "6")
                    start_day, start_hhmm = s1.split()
                    end_day, end_hhmm = s2.split()
                    tz = _device_tz(cfg)
                    start_utc, end_utc = local_window_to_utc(start_day, start_hhmm, end_day, end_hhmm, tz)
                    start_dt = datetime.fromisoformat(start_utc.replace("Z", ""))
                    end_dt = datetime.fromisoformat(end_utc.replace("Z", ""))
                    n = backfill_range(svc, start_dt, end_dt, chunk_hours=chunk)
                    print(f"Backfill ventana OK. RRHH insertados: {n}")
                    print(f"UTC: {start_utc} -> {end_utc}")

                elif op == "7":
                    print("\nImportar JSONL (1 JSON por línea).")
                    print("Sugerencia: pon el archivo en la misma carpeta del programa.")
                    path = _input("Ruta del archivo JSONL (ej events_20260207.jsonl.txt): ")
                    raw_n, rrhh_n, err_n = import_jsonl_file(
                        svc,
                        jsonl_path=path,
                        device_ip_fallback=cfg.device.get("ip"),
                        persist_to_db=True,
                    )
                    print("\nImport OK")
                    print(f"RAW insertados: {raw_n}")
                    print(f"RRHH insertados (dedupe): {rrhh_n}")
                    print(f"Errores: {err_n}")

                elif op == "8":
                    print("\nImportar JSONL (1 JSON por línea) y luego Exportar.")
                    print("Sugerencia: pon el archivo en la misma carpeta del programa.")
                    path = _input("Ruta del archivo JSONL (ej events_20260207.jsonl.txt): ")
                    raw_n, rrhh_n, err_n = import_jsonl_file(
                        svc,
                        jsonl_path=path,
                        device_ip_fallback=cfg.device.get("ip"),
                        persist_to_db=True,
                    )
                    print("\nImport OK")
                    print(f"RAW insertados: {raw_n}")
                    print(f"RRHH insertados (dedupe): {rrhh_n}")
                    print(f"Errores: {err_n}")


                    _press_enter()
                    menu_export()
                    continue

                else:
                    print("Opción inválida")

            except Exception as e:
                print(f"Backfill error: {e}")

            _press_enter()

    def menu_export():
        while True:
            print("\n=== Export ===")
            print("1) Diario por día operativo (YYYY-MM-DD) -> Excel resumen (1 hoja)")
            print("2) Rango de días operativos (YYYY-MM-DD a YYYY-MM-DD) -> Excel resumen (1 hoja)")
            print("6) Diario (atajo) -> Excel resumen (1 hoja)")
            print("7) Rebuild/Indexar jornadas en DB (jornada_id estable)")
            print("8) Importar CORRECCIONES (Excel) -> guardar overrides y rebuild automático (NO borra patrones)")
            print("0) Volver")

            op = _input("Opción: ")
            if op == "0":
                return

            try:
                if op == "8":
                    source = "db"
                    label = "DB"
                else:
                    source = choose_export_source()
                    label = source_label(source)
                out_dir = os.path.join(cfg.storage["data_dir"], "exports")
                template = cfg.export.get("excel_template", "") or ""

                if source != "db":
                    print("Nota: el modo estable requiere usar la DB como fuente (source=DB).")

                tz = _device_tz(cfg)
                cutoff = "03:00"
                if hasattr(cfg, "operation"):
                    cutoff = (cfg.operation.get("shift_cutoff") or cutoff).strip() or cutoff

                break_max = int(cfg.export.get("break_max_minutes", 75))
                rest_min = int(cfg.export.get("min_rest_between_shifts_minutes", 240))
                debounce = int(cfg.export.get("debounce_minutes", 3))
                max_shift = int(cfg.export.get("max_shift_hours", 24))
                lookahead = (cfg.export.get("close_lookahead_hhmm", "12:00") or "12:00").strip()
                hybrid_close = cfg.export.get("hybrid_close", {}) or {}

                adv = (hybrid_close or {}).get("advanced_learning") or {}
                adv_audit = (adv or {}).get("audit") or {}
                audit_export_excel = bool((adv_audit or {}).get("export_excel", False))

                def _end_utc_for_date(d: str, hhmm: str) -> str:
                    ed = datetime.strptime(d, "%Y-%m-%d").date()
                    h, m = [int(x) for x in hhmm.split(":")]
                    end_local = datetime(ed.year, ed.month, ed.day, h, m, 0, tzinfo=tz)
                    return end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"

                if op == "8":
                    excel_path = _input("Ruta del Excel (export con CORRECCIONES): ")
                    res = import_manual_corrections_excel(svc.db, excel_path)
                    # Rebuild completo (replay) para aplicar las correcciones.
                    last_utc = (svc.db.get_last_processed_event_time_utc() or "").strip()
                    if not last_utc:
                        raise ValueError("No hay eventos procesados en la DB para rebuild")
                    try:
                        dt_last = datetime.fromisoformat(last_utc.replace("Z", ""))
                    except Exception:
                        raise ValueError(f"Formato inválido event_time_utc: {last_utc}")
                    end_utc = (dt_last + timedelta(minutes=1)).isoformat(timespec="seconds") + "Z"
                    info = ensure_jornadas_indexed_until(
                        svc.db,
                        end_utc,
                        device_tz=tz,
                        cutoff_hhmm=cutoff,
                        break_max_minutes=break_max,
                        rest_min_minutes=rest_min,
                        debounce_minutes=debounce,
                        max_shift_hours=max_shift,
                        rebuild=True,
                        hybrid_close=hybrid_close,
                    )
                    print("OK. Correcciones importadas:")
                    print(f"  export_id: {res.export_id}")
                    print(f"  importadas: {res.imported} | omitidas: {res.skipped} | errores: {res.errors}")
                    if res.decisions:
                        print(f"  decisiones: {res.decisions}")
                    print("OK. Rebuild completado (no borra patrones).")
                    print(f"Indexados: {info.get('indexed')} (hasta {info.get('end_utc')})")
                    continue

                if op in ("1", "6"):
                    day = _input("Día operativo (YYYY-MM-DD): ")
                    d0 = datetime.strptime(day, "%Y-%m-%d").date()
                    end_day = (d0 + timedelta(days=1)).isoformat()


                    end_utc = _end_utc_for_date(end_day, lookahead)
                    info = ensure_jornadas_indexed_until(
                        svc.db,
                        end_utc,
                        device_tz=tz,
                        cutoff_hhmm=cutoff,
                        break_max_minutes=break_max,
                        rest_min_minutes=rest_min,
                        debounce_minutes=debounce,
                        max_shift_hours=max_shift,
                        rebuild=False,
                        hybrid_close=hybrid_close,
                    )
                    jornadas = svc.db.get_jornadas_by_op_date(day)
                    rows = jornadas_to_export_rows(jornadas, collapse_single_event_blocks=True)

                    name = f"Eventos-{day}_a_{end_day}_{label}"
                    extra = None
                    if audit_export_excel:
                        extra = {"AUDIT_MODEL": svc.db.get_model_audit_range(day, day)}
                    export_id = str(uuid.uuid4())
                    try:
                        svc.db.insert_export_log(
                            export_id,
                            range_start_op=day,
                            range_end_op=day,
                            source_label=label,
                            file_name=f"{name}.xlsx",
                        )
                    except Exception:
                        pass
                    meta = {
                        "range_start_op": day,
                        "range_end_op": day,
                        "source_label": label,
                        "file_name": f"{name}.xlsx",
                    }
                    xp = export_excel_jornadas_summary(
                        rows,
                        out_dir,
                        name,
                        template_path=template,
                        extra_sheets=extra,
                        export_id=export_id,
                        meta=meta,
                    )
                    print(f"Indexados: {info.get('indexed')} (hasta {info.get('end_utc')})")
                    print(f"Excel: {xp}")

                elif op == "2":
                    start = _input("Inicio (YYYY-MM-DD): ")
                    end = _input("Fin (YYYY-MM-DD): ")
                    d1 = datetime.strptime(start, "%Y-%m-%d").date()
                    d2 = datetime.strptime(end, "%Y-%m-%d").date()
                    if d2 < d1:
                        raise ValueError("Fin debe ser >= Inicio")

                    end_day = (d2 + timedelta(days=1)).isoformat()
                    end_utc = _end_utc_for_date(end_day, lookahead)
                    info = ensure_jornadas_indexed_until(
                        svc.db,
                        end_utc,
                        device_tz=tz,
                        cutoff_hhmm=cutoff,
                        break_max_minutes=break_max,
                        rest_min_minutes=rest_min,
                        debounce_minutes=debounce,
                        max_shift_hours=max_shift,
                        rebuild=False,
                        hybrid_close=hybrid_close,
                    )

                    all_rows = []
                    cur = d1
                    while cur <= d2:
                        op_day = cur.isoformat()
                        jornadas = svc.db.get_jornadas_by_op_date(op_day)
                        all_rows.extend(jornadas_to_export_rows(jornadas, collapse_single_event_blocks=True))
                        cur += timedelta(days=1)
                    name = f"Eventos-{start}_a_{end}_{label}"
                    extra = None
                    if audit_export_excel:
                        extra = {"AUDIT_MODEL": svc.db.get_model_audit_range(start, end)}
                    export_id = str(uuid.uuid4())
                    try:
                        svc.db.insert_export_log(
                            export_id,
                            range_start_op=start,
                            range_end_op=end,
                            source_label=label,
                            file_name=f"{name}.xlsx",
                        )
                    except Exception:
                        pass
                    meta = {
                        "range_start_op": start,
                        "range_end_op": end,
                        "source_label": label,
                        "file_name": f"{name}.xlsx",
                    }
                    xp = export_excel_jornadas_summary(
                        all_rows,
                        out_dir,
                        name,
                        template_path=template,
                        extra_sheets=extra,
                        export_id=export_id,
                        meta=meta,
                    )
                    print(f"Indexados: {info.get('indexed')} (hasta {info.get('end_utc')})")
                    print(f"Excel: {xp}")

                elif op == "7":
                    start = _input("Inicio (YYYY-MM-DD): ")
                    end = _input("Fin (YYYY-MM-DD): ")
                    d1 = datetime.strptime(start, "%Y-%m-%d").date()
                    d2 = datetime.strptime(end, "%Y-%m-%d").date()
                    if d2 < d1:
                        raise ValueError("Fin debe ser >= Inicio")

                    svc.db.clear_jornadas(preserve_patterns=True)
                    try:
                        svc.db.reset_employee_state_preserve_patterns()
                    except Exception:
                        pass
                    start_ctx = (d1 - timedelta(days=1)).isoformat()
                    start_ctx_utc = local_window_to_utc(start_ctx, cutoff, start_ctx, cutoff, tz)[0]
                    svc.db.upsert_state("jornada_index_last_utc", start_ctx_utc)
                    end_day = (d2 + timedelta(days=1)).isoformat()
                    end_utc = _end_utc_for_date(end_day, lookahead)
                    info = ensure_jornadas_indexed_until(
                        svc.db,
                        end_utc,
                        device_tz=tz,
                        cutoff_hhmm=cutoff,
                        break_max_minutes=break_max,
                        rest_min_minutes=rest_min,
                        debounce_minutes=debounce,
                        max_shift_hours=max_shift,
                        rebuild=False,
                        hybrid_close=hybrid_close,
                    )
                    print("OK. Rebuild completado.")
                    print(f"Indexados: {info.get('indexed')} (hasta {info.get('end_utc')})")

                else:
                    print("Opción inválida")

            except Exception as e:
                print(f"Export error: {e}")

            _press_enter()

    while True:
        print("\n=== ISAPI Collector CLI ===")
        print("1) Iniciar recolección en tiempo real (24/7)")
        print("2) Iniciar recolección normal (24/7)")
        print("3) Pull único (una corrida)")
        print("4) Backfill (histórico del checador)")
        print("5) Export")
        print("6) Consultar eventos RRHH por fecha (YYYY-MM-DD)")
        print("7) Estado del sistema")
        print("8) Detener servicio")
        print("9) Supervisor RRHH (presencia / reportes)")
        print("0) Salir")

        op = _input("Opción: ")

        if op == "1":
            start_bg(realtime=True)
        elif op == "2":
            start_bg(realtime=False)
        elif op == "3":
            try:
                inserted = svc.pull_once()
                print(f"OK. Insertados RRHH: {inserted}")
            except Exception as e:
                print(f"Error: {e}")
            _press_enter()
        elif op == "4":
            menu_backfill()
        elif op == "5":
            menu_export()
        elif op == "6":
            day = _input("Fecha (YYYY-MM-DD): ")
            rrhh = svc.db.get_processed_by_date(day)
            print(f"RRHH: {len(rrhh)}")
            for ev in rrhh[:50]:
                print(f"- {ev.get('event_time')} | emp={ev.get('employee_id')} {ev.get('employee_name') or ''} | type={ev.get('event_type')}")
            if len(rrhh) > 50:
                print(f"... {len(rrhh)-50} más")
            _press_enter()
        elif op == "9":
            try:
                run_rrhh_supervisor(cfg_path or "config.json")
            except Exception as e:
                print(f"Error Supervisor RRHH: {e}")
            _press_enter()
        elif op == "7":
            show_status()
            _press_enter()
        elif op == "8":
            stop_bg()
            _press_enter()
        elif op == "0":
            stop_bg()
            svc.db.close()
            break
        else:
            print("Opción inválida")
            _press_enter()