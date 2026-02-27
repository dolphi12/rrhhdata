# Supervisor RRHH URSOMEX (v4WINI)

Supervisor de asistencia para RRHH/Operación: monitoreo en tiempo real + búsqueda por empleado + reportes PDF/Excel, leyendo datos del Colector ISAPI.

## Cambios clave v4WINI (seguridad y robustez)
- **DB del colector en modo solo lectura**: el supervisor no crea tablas ni escribe en la DB del colector.
- **DB secundaria RRHH**: roster, perfiles/patrones y estado interno se guardan en una DB aparte (default: `data/rrhh_store.sqlite3`).

Esto reduce riesgo de bloqueos/corrupción y evita “contaminar” la DB del colector con tablas RRHH.


## Novedad v4WINI
- **Nuevo reporte PDF: Lista de asistencia por semana operativa (Mié a Mar)** con totales P/A por empleado.
## Ejecución
1) `pip install -r requirements.txt`
2) Edita `config_rrhh.json`
3) `python ejecutar_rrhh.py -c config_rrhh.json`

## Configuración (config_rrhh.json)

### Conexión a DB del colector (solo lectura)
- `collector_database.engine`: `sqlite` o `postgres`
- `collector_database.sqlite_path`: ruta a la DB del colector (si sqlite)
- `collector_database.postgres_dsn`: DSN Postgres (si aplica)
- `mode.collector_read_only`: recomendado `true`

### DB secundaria RRHH (roster/perfiles)
- `rrhh_store.engine`: `sqlite` recomendado
- `rrhh_store.sqlite_path`: default `data/rrhh_store.sqlite3`

### Operación y analítica
- `operation.shift_cutoff_hhmm`: corte de vista diaria (default `03:00`)
- `presence.stale_after_minutes`: umbral “stale”
- `analytics.windows_days`: ventanas para perfiles
- `predictions.window_days`: ventana histórica para predicción

## Notas sobre 24/7 y cruces de madrugada
- El supervisor muestra:
  - **Vista por corte** (rango horario) para “eventos del día”.
  - **Vista por op_date del colector** (asignación de jornada) para evitar confusiones con cierres tardíos.
- La decisión de si un evento de madrugada es cierre del día anterior o inicio del día nuevo la hace el Colector; el Supervisor la respeta en reportes.



## Permisos (días no laborables justificados)
Algunos empleados pueden tener **permiso** para no laborar un día (no cuenta como ausencia).

### Cargar permisos desde CSV (menú opción 11)
Formato recomendado (encabezados):
- `employee_id` (o `id`)
- `op_date` (o `date` / `day`) en formato `YYYY-MM-DD`
- `reason` (opcional)

Ejemplo:
```csv
employee_id,op_date,reason
0881,2026-02-14,Permiso personal
0100,2026-02-15,Vacaciones
```

En reportes:
- En **Lista semana operativa** y **Excel asistencias**, el permiso se marca como **PR**.
- **PR no cuenta como ausencia** (no afecta el % de asistencia).
