from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from rrhh_supervisor.config import load_config
from rrhh_supervisor.storage.db import DB
from rrhh_supervisor.services.presence import compute_presence, summarize_presence
from rrhh_supervisor.services.employee import employee_day_view, operational_bounds_now
from rrhh_supervisor.services.analytics import build_employee_profile
from rrhh_supervisor.services.roster import load_roster_csv
from rrhh_supervisor.services.permissions import load_permissions_csv
from rrhh_supervisor.services.predict import predict_next_event
from rrhh_supervisor.services.global_report import build_global_report_data
from rrhh_supervisor.reports.attendance_excel import export_attendance_matrix
from rrhh_supervisor.reports.employee_pdf import render_employee_pdf
from rrhh_supervisor.reports.weekly_attendance_pdf import operational_week_bounds, render_weekly_attendance_pdf
from rrhh_supervisor.reports.global_pdf import render_global_pdf


def _menu() -> str:
    print("")
    print("Supervisor RRHH URSOMEX")
    print("1) Resumen en tiempo real (laborando/pausa/fuera) + pendientes de iniciar")
    print("2) Buscar empleado (ID o nombre) y ver detalle del día actual")
    print("3) Ver eventos del día actual (operativo) por empleado")
    print("4) Recalcular perfil/patrones de un empleado")
    print("5) Exportar PDF (reporte por empleado)")
    print("6) Exportar Excel (asistencias por día)")
    print("7) Cargar/actualizar roster desde CSV")
    print("8) Exportar PDF (reporte global)")
    print("9) Recalcular perfiles masivo")
    print("10) Exportar PDF (lista asistencia semana operativa)")
    print("11) Cargar/actualizar permisos (días no laborables) desde CSV")
    print("0) Salir")
    return input("Opción: ").strip()


def _ensure_out_dir(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)


def _now_local(local_tz: str) -> datetime:
    tz = ZoneInfo(local_tz)
    return datetime.now(tz=tz)


def _fmt_minutes(m: Any) -> str:
    if m is None:
        return "-"
    try:
        mm = int(m)
    except Exception:
        return "-"
    if mm < 60:
        return f"{mm}m"
    h = mm // 60
    r = mm % 60
    return f"{h}h {r:02d}m"


def _search_employees_any(collector: DB, store: DB, q: str, limit: int = 25) -> List[Dict[str, Any]]:
    s = (q or "").strip()
    if not s:
        return []
    out: Dict[str, Dict[str, Any]] = {}
    for r in store.search_roster(s, limit=int(limit)):
        emp = str(r.get("employee_id") or "").strip()
        if not emp:
            continue
        out[emp] = {"employee_id": emp, "employee_name": str(r.get("employee_name") or "").strip()}
    if len(out) < int(limit):
        for r in collector.search_employees(s, limit=int(limit)):
            emp = str(r.get("employee_id") or "").strip()
            if not emp or emp in out:
                continue
            out[emp] = {"employee_id": emp, "employee_name": str(r.get("employee_name") or "").strip()}
    items = list(out.values())
    items.sort(key=lambda x: x.get("employee_id") or "")
    return items[: int(limit)]


def _pending_start_breakdown(
    collector: DB,
    store: DB,
    local_tz: str,
    cutoff_hhmm: str,
    entry_window_minutes: int,
    window_days: int,
    min_jornadas_for_profile: int,
) -> Dict[str, Any]:
    op_start, op_end, op_date = operational_bounds_now(local_tz, cutoff_hhmm)
    now = _now_local(local_tz)
    started = set(collector.list_employee_ids_with_opdate(op_date))
    roster = store.list_roster(active_only=True)
    if roster:
        base = [str(r.get("employee_id") or "").strip() for r in roster if r.get("employee_id")]
    else:
        since_iso = (datetime.now(tz=ZoneInfo("UTC")) - timedelta(days=60)).replace(microsecond=0).isoformat().replace("+00:00","Z")
        base = [str(r.get("employee_id") or "").strip() for r in collector.list_active_employees_last_days(since_iso) if r.get("employee_id")]
    base = [x for x in base if x]

    counts = {"ROSTER": len(base), "INICIARON": 0, "SIN_INICIAR": 0, "ESPERADO": 0, "RETRASADO": 0, "SIN_PERFIL": 0}
    counts["INICIARON"] = sum(1 for e in base if e in started)

    for emp in base:
        if emp in started:
            continue
        counts["SIN_INICIAR"] += 1

        prof = store.get_employee_profile(emp, window_days)
        if prof is None:
            prof = build_employee_profile(collector, emp, local_tz, window_days, min_jornadas_for_profile)
            if prof is not None:
                store.upsert_employee_profile(emp, window_days, prof)

        if not prof or not prof.get("entry", {}) or not prof["entry"].get("median"):
            counts["SIN_PERFIL"] += 1
            continue

        hhmm = str(prof["entry"]["median"])
        try:
            h, m = hhmm.split(":")
            expected = datetime.fromisoformat(f"{op_date}T{int(h):02d}:{int(m):02d}:00").replace(tzinfo=ZoneInfo(local_tz))
        except Exception:
            counts["SIN_PERFIL"] += 1
            continue

        if now <= expected + timedelta(minutes=int(entry_window_minutes)):
            counts["ESPERADO"] += 1
        else:
            counts["RETRASADO"] += 1

    return {"op_date": op_date, "counts": counts}


def run(config_path: str):
    cfg = load_config(config_path)

    collector = DB(
        cfg.collector_db_engine,
        cfg.collector_sqlite_path,
        cfg.collector_postgres_dsn,
        init_rrhh_schema=False,
        sqlite_read_only=cfg.collector_read_only,
    )
    store = DB(
        cfg.store_db_engine,
        cfg.store_sqlite_path,
        cfg.store_postgres_dsn,
        init_rrhh_schema=True,
        sqlite_read_only=False,
    )

    try:
        while True:
            op = _menu()
            if op == "0":
                break

            if op == "1":
                rows = compute_presence(collector, cfg.local_tz, cfg.stale_after_minutes)
                s = summarize_presence(rows)
                pend = _pending_start_breakdown(
                    collector,
                    store,
                    cfg.local_tz,
                    cfg.shift_cutoff_hhmm,
                    cfg.entry_window_minutes,
                    cfg.analytics_windows_days[0],
                    cfg.min_jornadas_for_profile,
                )
                print("")
                print(f"Hora local: {_now_local(cfg.local_tz).replace(microsecond=0).isoformat()}")
                print(f"Día operativo (por corte): {pend['op_date']}")
                print(
                    f"Total con datos: {s['TOTAL']}  Laborando: {s['LABORANDO']}  Pausa: {s['PAUSA']}  Fuera: {s['FUERA']}  Incierto: {s['INCIERTO']}  Sin datos: {s['SIN_DATOS']}"
                )
                c = pend["counts"]
                print(
                    f"Roster/activos: {c['ROSTER']}  Iniciaron hoy: {c['INICIARON']}  Sin iniciar hoy: {c['SIN_INICIAR']}  Esperados: {c['ESPERADO']}  Retrasados: {c['RETRASADO']}  Sin perfil: {c['SIN_PERFIL']}"
                )
                print("")
                print("Top 30 (ordenado por minutos desde último evento):")
                rows2 = sorted(rows, key=lambda r: (r.minutes_since_last_event is None, -(r.minutes_since_last_event or 0)))
                for r in rows2[:30]:
                    print(f"{r.employee_id:>6}  {r.status:<10}  {r.last_role:<3}  {_fmt_minutes(r.minutes_since_last_event):>10}  {r.employee_name}")
                continue

            if op == "2":
                q = input("ID o nombre (parcial): ").strip()
                res = _search_employees_any(collector, store, q, limit=25)
                if not res:
                    print("Sin resultados.")
                    continue
                for i, r in enumerate(res, 1):
                    print(f"{i:>2}) {r['employee_id']}  {r.get('employee_name','')}")
                sel = input("Elige número para ver detalle (Enter para salir): ").strip()
                if not sel:
                    continue
                try:
                    idx = int(sel) - 1
                    emp = res[idx]["employee_id"]
                except Exception:
                    print("Selección inválida.")
                    continue

                latest = collector.get_latest_event_for_employee(emp)
                open_j = collector.get_open_jornada_for_employee(emp)
                rows = compute_presence(collector, cfg.local_tz, cfg.stale_after_minutes)
                pr = next((r for r in rows if r.employee_id == emp), None)

                print("")
                print(f"Empleado: {emp}  {res[idx].get('employee_name','')}")
                if pr is not None:
                    print(f"Estado: {pr.status}   Último rol: {pr.last_role}   Último evento UTC: {pr.last_event_utc}")
                elif latest:
                    print(f"Estado: SIN_DATOS   Último rol: {latest.get('role')}   Último evento UTC: {latest.get('event_time_utc')}")
                else:
                    print("Estado: SIN_DATOS")

                if open_j:
                    print(f"Jornada abierta: {open_j.get('jornada_id')}   OpDate (colector): {open_j.get('op_date')}   Inicio UTC: {open_j.get('start_time_utc')}")

                prof_wd = cfg.analytics_windows_days[0]
                prof = store.get_employee_profile(emp, prof_wd)
                if prof is None:
                    prof = build_employee_profile(collector, emp, cfg.local_tz, prof_wd, cfg.min_jornadas_for_profile)
                    if prof is not None:
                        store.upsert_employee_profile(emp, prof_wd, prof)

                pred = predict_next_event(
                    collector,
                    emp,
                    cfg.local_tz,
                    cfg.predictor_window_days,
                    cfg.entry_window_minutes,
                    cfg.confidence_min_samples,
                )

                if pred is not None and pred.expected_role and pred.expected_time_local:
                    print(
                        f"Próximo evento estimado: {pred.expected_role}  {pred.expected_time_local}  (confianza {pred.confidence}, muestras {pred.samples})"
                    )

                v = employee_day_view(collector, emp, cfg.local_tz, cfg.shift_cutoff_hhmm)
                print("")
                print(f"Día calendario: {v['calendar_date']}   Día operativo (por corte): {v['op_date']}")

                print("")
                print("Eventos hoy (operativo por corte):")
                for e in v["events_operational"]:
                    print(f"  {e['event_time_utc']}  {e['role']}")

                print("")
                print("Eventos asociados a op_date (según colector):")
                for e in v.get("events_by_op_date", []):
                    print(f"  {e['event_time_utc']}  {e['role']}  (op_date {e.get('op_date')})")

                print("")
                print("Eventos hoy (calendario):")
                for e in v["events_calendar"]:
                    print(f"  {e['event_time_utc']}  {e['role']}")
                continue

            if op == "3":
                emp = input("Empleado ID: ").strip()
                if not emp:
                    continue
                v = employee_day_view(collector, emp, cfg.local_tz, cfg.shift_cutoff_hhmm)
                print("")
                print(f"Empleado: {emp}")
                print(f"Día calendario: {v['calendar_date']}   Día operativo (por corte): {v['op_date']}")
                print("")
                print("Eventos hoy (operativo por corte):")
                for e in v["events_operational"]:
                    print(f"  {e['event_time_utc']}  {e['role']}")
                print("")
                print("Eventos asociados a op_date (según colector):")
                for e in v.get("events_by_op_date", []):
                    print(f"  {e['event_time_utc']}  {e['role']}  (op_date {e.get('op_date')})")
                continue

            if op == "4":
                emp = input("Empleado ID: ").strip()
                if not emp:
                    continue
                for wd in cfg.analytics_windows_days:
                    prof = build_employee_profile(collector, emp, cfg.local_tz, wd, cfg.min_jornadas_for_profile)
                    if prof is None:
                        print(f"{wd} días: sin suficientes jornadas para perfil.")
                        continue
                    store.upsert_employee_profile(emp, wd, prof)
                    print(f"{wd} días: perfil actualizado.")
                continue

            if op == "5":
                emp = input("Empleado ID: ").strip()
                if not emp:
                    continue
                wd_s = input(f"Ventana en días (cualquier número, default {cfg.analytics_windows_days[0]}): ").strip()
                wd = cfg.analytics_windows_days[0]
                if wd_s:
                    try:
                        wd = int(wd_s)
                    except Exception:
                        wd = cfg.analytics_windows_days[0]

                if int(wd) < 1:
                    wd = cfg.analytics_windows_days[0]

                prof = store.get_employee_profile(emp, wd)
                if prof is None:
                    prof = build_employee_profile(collector, emp, cfg.local_tz, wd, cfg.min_jornadas_for_profile)
                    if prof is None:
                        print("No hay perfil disponible (insuficientes datos).")
                        continue
                    store.upsert_employee_profile(emp, wd, prof)

                now_utc = datetime.now(tz=ZoneInfo("UTC"))
                start_utc = now_utc - timedelta(days=int(wd))
                start_iso = start_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                end_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                jornadas = collector.list_jornadas_closed_range(emp, start_iso, end_iso)

                out_dir = cfg.out_dir
                _ensure_out_dir(out_dir)
                out_path = os.path.join(out_dir, f"Reporte_Empleado_{emp}_{wd}d.pdf")
                render_employee_pdf(out_path, collector, cfg.local_tz, cfg.shift_cutoff_hhmm, prof, jornadas)
                print(f"PDF generado: {out_path}")
                continue

            if op == "6":
                start_date = input("Fecha inicio (YYYY-MM-DD): ").strip()
                end_date = input("Fecha fin (YYYY-MM-DD): ").strip()
                out_dir = cfg.out_dir
                _ensure_out_dir(out_dir)
                out_path = os.path.join(out_dir, f"Asistencias_{start_date}_a_{end_date}.xlsx")
                export_attendance_matrix(collector, store, cfg.local_tz, start_date, end_date, out_path)
                print(f"Excel generado: {out_path}")
                continue

            if op == "7":
                path = input(f"Ruta CSV (Enter = {cfg.roster_csv_path}): ").strip() or cfg.roster_csv_path
                if not path:
                    print("Ruta CSV requerida.")
                    continue
                try:
                    recs = load_roster_csv(path, cfg.roster_id_min_width, cfg.roster_default_active)
                except Exception as e:
                    print(f"Error leyendo CSV: {e}")
                    continue
                store.upsert_roster(recs, source="csv")
                print(f"Roster actualizado: {len(recs)} registros.")
                continue
            if op == "11":
                path = input("Ruta CSV permisos (employee_id, op_date, reason): ").strip()
                if not path:
                    print("Ruta CSV requerida.")
                    continue
                try:
                    recs = load_permissions_csv(path, cfg.roster_id_min_width)
                except Exception as e:
                    print(f"Error leyendo CSV permisos: {e}")
                    continue
                store.upsert_permissions(recs, source="csv")
                print(f"Permisos actualizados: {len(recs)} registros.")
                continue


            if op == "8":
                start_date = input("Fecha inicio (YYYY-MM-DD): ").strip()
                end_date = input("Fecha fin (YYYY-MM-DD): ").strip()
                out_dir = cfg.out_dir
                _ensure_out_dir(out_dir)
                data = build_global_report_data(collector, store, start_date, end_date, cfg.roster_only_active)
                out_path = os.path.join(out_dir, f"Reporte_Global_{start_date}_a_{end_date}.pdf")
                render_global_pdf(out_path, data, cfg.local_tz)
                print(f"PDF generado: {out_path}")
                continue


            if op == "10":
                ref = input("Fecha de referencia (YYYY-MM-DD) [vacío = hoy]: ").strip()
                if not ref:
                    ref_date = datetime.now().date()
                else:
                    try:
                        ref_date = date.fromisoformat(ref)
                    except Exception:
                        print("Fecha inválida. Usa formato YYYY-MM-DD.")
                        continue
                wk_start, wk_end = operational_week_bounds(ref_date)
                roster = store.list_roster(active_only=True)
                jornadas = collector.list_jornadas_closed_opdate_range(wk_start.isoformat(), wk_end.isoformat())
                out_dir = cfg.out_dir
                _ensure_out_dir(out_dir)
                out_path = os.path.join(out_dir, f"Asistencia_SemanaOperativa_{wk_start.isoformat()}_a_{wk_end.isoformat()}.pdf")
                perms = store.permissions_set_opdate_range(wk_start.isoformat(), wk_end.isoformat())
                render_weekly_attendance_pdf(out_path, wk_start, wk_end, roster, jornadas, cfg.local_tz, db=collector, permissions_map=perms)
                print(f"PDF generado: {out_path}")
                continue

            if op == "9":
                roster = store.list_roster(active_only=True)
                if roster:
                    emps = [str(r.get("employee_id") or "").strip() for r in roster if r.get("employee_id")]
                else:
                    since_iso = (datetime.now(tz=ZoneInfo("UTC")) - timedelta(days=60)).replace(microsecond=0).isoformat().replace("+00:00","Z")
                    base = [str(r.get("employee_id") or "").strip() for r in collector.list_active_employees_last_days(since_iso) if r.get("employee_id")]
                emps = [e for e in emps if e]
                print(f"Actualizando perfiles para {len(emps)} empleados...")
                ok = 0
                for emp in emps:
                    for wd in cfg.analytics_windows_days:
                        prof = build_employee_profile(collector, emp, cfg.local_tz, wd, cfg.min_jornadas_for_profile)
                        if prof is None:
                            continue
                        store.upsert_employee_profile(emp, wd, prof)
                    ok += 1
                print(f"Listo. Empleados procesados: {ok}")
                continue

            print("Opción inválida.")
    finally:
        try:
            collector.close()
        finally:
            store.close()


def run_with_config_dict(data: Dict[str, Any], _tmp_path: str = "storage/.rrhh_runtime_config.json"):
    """Ejecuta el supervisor RRHH con un dict de configuración.

    Escribe un JSON temporal para reutilizar la lógica original del supervisor.
    """
    os.makedirs(os.path.dirname(_tmp_path) or ".", exist_ok=True)
    tmp = json.dumps(data, ensure_ascii=False, indent=2)
    with open(_tmp_path, "w", encoding="utf-8") as f:
        f.write(tmp)
    run(_tmp_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", required=True, help="Ruta de config_rrhh.json")
    args = ap.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
