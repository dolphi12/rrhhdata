# ISAPI Collector CLI — Checador Asistencias (Hikvision)

Sistema Python CLI para **recolección 24/7** de eventos por **ISAPI**, almacenamiento en **SQLite/PostgreSQL** y exportación a **Excel + JSON**.

El objetivo operativo es:
- Mantener un histórico de eventos crudos (con timestamp completo).
- Generar exportaciones de RRHH limpias y consistentes.
- Soportar operación 24/7 con cruces de medianoche (incluye temporada alta).

## Requisitos
- Windows 10/11
- Python 3.10+
- Paquetes:
  - `requests`
  - `openpyxl`
  - `psycopg` (solo si usas PostgreSQL)

Instalación:
```bash
pip install -r requirements.txt
```

## Configuración
Edita `config.json`:
- `device.ip`, `device.user`, `device.password`
- `database.engine` = `sqlite` o `postgres`
- `operation.shift_cutoff` (por defecto `03:00`)

## Ejecución
```bash
python ejecutar.py -c config.json
```

## Export y reportes diarios
En el menú **Export** puedes elegir la fuente:
- **DB**: exporta lo ya guardado y procesado.
- **Checador Asistencias**: extrae, guarda y exporta en un flujo.

La exportación genera:
- `RRHH_PROCESSED` (eventos procesados)
- `AUDIT_ALL_EVENTS` (auditoría)
- `JORNADAS_CIERRE` (resumen por empleado; una fila por empleado y día operativo)

### Día operativo y corte 03:00
El sistema usa el concepto de **día operativo**. Con `operation.shift_cutoff = 03:00`:
- Los eventos entre `00:00` y `02:59` se consideran del **día operativo anterior**.
- Los eventos desde `03:00` en adelante se consideran del **día operativo del mismo calendario**.

Esto evita que RRHH vea “dos días” cuando realmente es una sola jornada que cruzó medianoche.

### Indexado estable de jornadas (jornada_id)
En DB se construyen jornadas por empleado con un `jornada_id` estable.
Reglas principales:
- `export.debounce_minutes`: ignora duplicados muy cercanos.
- `export.break_max_minutes`: pausas normales (comida/cena) no cierran jornada.
- `export.min_rest_between_shifts_minutes`: si un empleado descansa >= este valor y luego aparece un evento IN, se considera nueva jornada.
- `export.max_shift_hours`: si una jornada queda “abierta” demasiado tiempo, se cierra para evitar que se coma jornadas completas.

### Cierre híbrido para temporada alta (salidas hasta 07:30 y entradas desde 04:00)
En temporada alta puede ocurrir que:
- Un empleado termine su jornada en la madrugada del día siguiente (salida real hasta `07:30`).
- Otro empleado inicie una nueva jornada desde `04:00` (y presenta múltiples eventos seguidos).

Para resolver la ambigüedad se usa `export.hybrid_close`:

```json
"hybrid_close": {
  "enabled": true,
  "entry_start_hhmm": "04:00",
  "close_window_end_hhmm": "07:30",
  "late_threshold_hhmm": "18:00",
  "anti_fp_window_minutes": 90,
  "anti_fp_min_additional_events": 2,
  "require_late_signature": true,
  "max_join_shift_hours": 24
}
```

Comportamiento:
1) Si la jornada del día D tuvo una “firma tarde” (>= `late_threshold_hhmm`) y aparece un evento en D+1 dentro de la ventana (`03:00` a `07:30`), ese primer evento puede ser cierre.
2) **Anti-falso-positivo**: si después de ese primer evento en D+1 aparecen **2 o más eventos adicionales** dentro de `anti_fp_window_minutes` (por defecto 90 min), se clasifica como **ENTRADA** (inicio de nueva jornada) y no como cierre.
3) Si pasa los filtros, el sistema toma **solo 1 evento de D+1** como **fin de jornada** del día D, y el resto de eventos de D+1 quedan para la nueva jornada.

Ejemplo típico:
- Día D: 06:00, 12:00, 13:00, 17:00, 18:00
- Día D+1:
  - Caso A (cierre real): 04:00 y luego nada cercano -> 04:00 se usa como **salida** del día D.
  - Caso B (ya inició nueva jornada): 04:00, 04:30, 05:00, 05:30... -> 04:00 se trata como **entrada** de la nueva jornada.

## Notas
- El checador Hikvision suele requerir **HTTP Digest Auth**.
- El endpoint ISAPI puede variar según firmware. Si tu equipo no responde con el endpoint actual, agrega el correcto en `device.endpoints`.
- Para mantenimiento de DB (SQLite), se recomienda respaldar periódicamente usando `storage.backup_dir`.

## Notas de versión

- Corrección: se agregó el parser interno de HH:MM requerido por el indexador de jornadas (evita el error `name '_hhmm_to_time' is not defined` durante exportación).


## Nota sobre Rebuild/Index (opción 7)

La opción **7) Rebuild/Indexar jornadas** recalcula las tablas derivadas (`jornadas`, `jornada_events`) desde los eventos crudos en DB.
Desde esta versión, **NO borra los patrones/estado aprendido por empleado** (tabla `employee_jornada_state`) por defecto.

- Para conservar aprendizaje: usa Rebuild normal.
- Si necesitas un borrado total de patrones, elimina manualmente `employee_jornada_state` (solo en casos especiales).


## Aprendizaje avanzado — “Nivel 2” estable (temporada alta/baja)

Además de los patrones por empleado (2/4/5/6 checadas, permisos, duplicados), el sistema puede ajustar automáticamente la **confianza de “cierre al día siguiente (D+1)”** según la *estacionalidad*.

### ¿Qué detecta?
Un **régimen PEAK (temporada alta)** basado en dos señales estables:
- **Volumen diario**: jornadas por día operativo (jpd)
- **% de cierres D+1**: proporción de jornadas cuyo fin real cae en el día calendario siguiente

Estas señales se suavizan con **EWMA** (por defecto 7 días) y se comparan contra una línea base robusta (mediana + MAD). Para evitar “rebotes”, usa **histeresis**:
- Entra a PEAK si el índice IOI está alto **3 días seguidos**
- Sale de PEAK si el IOI está bajo **7 días seguidos**

### ¿Qué cambia en PEAK?
Solo cambian los **pesos** del prior (sin romper reglas duras):
- Offpeak: pesa más el perfil base (todo el año)
- Peak: pesa más el perfil *recent* (adaptación rápida) y algo menos el base

Esto permite que, al llegar temporada alta, el sistema se adapte sin borrar lo aprendido en baja.

### ¿Cómo sé si NO estoy reseteando patrones?
- Los patrones/estado por empleado viven en la tabla `employee_jornada_state`
- El modelo global/cluster vive en `system_state` (keys `model_global_profile`, `model_cluster_profiles`)
- El clustering v2 (k-means por ventana) se guarda en `system_state` como `model_cluster_v2_state`
- La estacionalidad v2 se guarda en `system_state` como `model_seasonality_v2_state`

En el **Dashboard** ahora se muestra:
- `Patrones/modelo guardados: X empleados ...`
- `Seasonality v2: peak_mode=True/False | IOI=... | max_op_date=...`

### Instalación en otra PC (sin “reset”)
En una máquina nueva:
1) **Backfill** (cargar histórico / DB)
2) **Rebuild/Indexar jornadas** una vez
3) A partir de ahí, operación normal (pull + export)

El Rebuild normal **no borra** `employee_jornada_state`, así que el aprendizaje se mantiene.


## Correcciones manuales desde Excel (Jornada) + aprendizaje

Si RRHH hace una corrección manual (por ejemplo, un evento del día siguiente que *debería pegar* al día anterior), el sistema puede **leer esa corrección** desde el mismo Excel y usarla como señal adicional.

### ¿Qué cambia en el Excel exportado?
El Excel `JORNADAS_CIERRE` incluye:
- Columna `jornada_uid` (identificador estable de la jornada)
- Hoja `CORRECCIONES` para capturar overrides
- Hoja oculta `__META` con `export_id` y rango

En `CORRECCIONES` agrega filas con:
- `jornada_uid`
- `accion` (dropdown): `FORZAR_CIERRE_D1` o `FORZAR_ENTRADA`
- `nota` (opcional)

### Importar y aplicar
En el menú **Export**:
- **8) Importar CORRECCIONES (Excel) -> guardar overrides y rebuild automático**

Esto:
1) Guarda el override en DB (`manual_labels` + historial en `manual_corrections`)
2) Ejecuta **rebuild completo** de `jornadas/jornada_events` (sin borrar patrones)
3) Las decisiones forzadas se usan como “ground truth” y el modelo/patrones se ajustan con el tiempo.
