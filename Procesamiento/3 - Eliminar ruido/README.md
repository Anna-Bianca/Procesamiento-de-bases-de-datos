# Paso 3: detección y eliminación auditable de ruido

Este paso separa deliberadamente la detección heurística de la eliminación. El
modo `audit` nunca crea un corpus limpio ni modifica registros: produce un CSV
completo para revisión humana. El modo `apply` vuelve a leer el JSONL original y
solo elimina candidatos cuya columna `decision` sea exactamente `eliminar`.

## Uso

```powershell
python "Procesamiento\3 - Eliminar ruido\3_Eliminar_ruido.py" --mode audit
```

Para reanudar un corpus grande en un equipo con 8 GB de RAM, usando lotes más
eficientes y mostrando actividad cada 15 segundos:

```powershell
python "Procesamiento\3 - Eliminar ruido\3_Eliminar_ruido.py" `
  --mode audit `
  --resume `
  --batch-size 2000 `
  --batch-max-mb 64 `
  --progress-every 2000 `
  --heartbeat-seconds 15 `
  --sqlite-cache-mb 256 `
  --sqlite-mmap-mb 1024
```

Si la detección ya llegó al 100 % y solo se quiere cerrar la auditoría sin crear
la muestra opcional, se puede reanudar agregando `--skip-sample`:

```powershell
python "Procesamiento\3 - Eliminar ruido\3_Eliminar_ruido.py" `
  --mode audit `
  --resume `
  --skip-sample `
  --heartbeat-seconds 15 `
  --sqlite-cache-mb 256 `
  --sqlite-mmap-mb 1024
```

Esto conserva `candidatos_ruido.csv` completo y genera el resumen final con
`tamano_muestra_generado` igual a cero. Para generar la muestra más adelante,
basta con volver a ejecutar `--mode audit --resume` sin `--skip-sample`; no se
repiten la indexación ni la detección.

Los mensajes `[ACTIVO]` son latidos informativos y pueden referirse a un lote
todavía no confirmado. La frase `checkpoint guardado` indica que ese avance ya
quedó persistido y que `--resume` continuará desde allí. Reducir
`--heartbeat-seconds` solo aumenta la frecuencia de los mensajes; no acelera el
procesamiento.

Después de revisar `candidatos_ruido.csv` y completar `decision` con
`eliminar`, `conservar` o dejarla vacía:

```powershell
python "Procesamiento\3 - Eliminar ruido\3_Eliminar_ruido.py" `
  --mode apply `
  --review-csv "Base de datos\3 - Eliminar ruido\candidatos_ruido.csv"
```

Los dos modos rechazan salidas existentes salvo que se use `--overwrite`, y
pueden continuar un lote confirmado con `--resume`. Los umbrales principales
son argumentos de CLI. `--disable-detector CODIGO` permite desactivar uno o más
detectores sin cambiar el flujo central.

### Revisión interactiva con Streamlit

La interfaz permite revisar un candidato por vez sin editar el CSV en Excel.
Muestra el fragmento exacto, contexto anterior y posterior, motivo, acción
propuesta, métricas e información de origen. Incluye decisiones `eliminar`,
`conservar` y pendiente, comentarios, filtros por motivo/origen/decisión,
contadores globales y una meta orientativa por sesión (20 por defecto).

Instalación aislada, desde la raíz del repositorio (una sola vez):

```powershell
python -m venv .venv-revision
.\.venv-revision\Scripts\python.exe -m pip install -r "Procesamiento\3 - Eliminar ruido\requirements-revision.txt"
```

Primer arranque, cuando la auditoría ya terminó:

```powershell
.\.venv-revision\Scripts\python.exe -m streamlit run "Procesamiento\3 - Eliminar ruido\revision_ruido_streamlit.py" --server.address 127.0.0.1
```

Para retomar otro día o después de una interrupción:

```powershell
.\.venv-revision\Scripts\python.exe -m streamlit run "Procesamiento\3 - Eliminar ruido\revision_ruido_streamlit.py" --server.address 127.0.0.1 -- --resume
```

Los argumentos propios de la interfaz van después de `--`, por ejemplo
`-- --resume --session-goal 15`. También acepta `--review-csv`, `--state-file`,
`--export-csv` y `--preview-chars`. Si Streamlit ya está instalado en el Python
habitual, se puede reemplazar el ejecutable del entorno por `python`.

La primera apertura verifica el CSV completo y lo importa por lotes a
`revision_ruido.sqlite3`, que actúa como checkpoint. `--resume` también retoma
una importación interrumpida. No se carga el CSV completo en RAM ni se relee el
JSONL de 24 GB para mostrar cada candidato. Al reiniciar se verifica nuevamente
el SHA-256 del CSV y del resumen; nunca se mezclan auditorías ni se descartan
decisiones previas. No hay opción de sobrescribir/reiniciar la revisión.

Para trabajar en sesiones cortas:

1. Elegir `Eliminar`, `Conservar` o pendiente y escribir comentarios opcionales.
2. Pulsar **Guardar y seguir** para persistir la decisión y pasar al siguiente.
   **Marcar pendiente y seguir** guarda las notas y deja la decisión vacía; no
   cuenta como revisión terminada. Las propuestas de registro completo requieren
   además una confirmación explícita para aprobar su eliminación.
3. Para pausar, pulsar **Guardar sin avanzar** y cerrar la pestaña o detener el
   servidor con `Ctrl+C`. Lo escrito pero no guardado no queda persistido.
4. Retomar con `--resume`: se conservan decisiones, comentarios y posición.

Los filtros no cambian el total global. El número de candidato es su posición
en el CSV original, no un contador de revisados. Un fragmento muy largo muestra
una vista previa con advertencia y opción de verlo completo: la decisión siempre
afecta a todo el intervalo auditado. Si otra pestaña modifica el mismo candidato,
se rechaza el guardado obsoleto para evitar pisar su decisión.

Cuando se quiera aplicar la revisión, pulsar **Exportar CSV completo para apply**.
Se genera `candidatos_ruido_revisados.csv` en la misma carpeta que
`auditoria_resumen.json`, incluyendo todos los candidatos, también los pendientes
y los ocultos por filtros. Solo cambian `decision` y `notas_revision`; se preservan
las demás columnas, IDs, hashes, offsets, Unicode y celdas multilínea. El original
`candidatos_ruido.csv` nunca se modifica. Se puede reexportar una salida propia
intacta, pero no sobrescribir archivos ajenos o modificados externamente.

```powershell
python "Procesamiento\3 - Eliminar ruido\3_Eliminar_ruido.py" `
  --mode apply `
  --review-csv "Base de datos\3 - Eliminar ruido\candidatos_ruido_revisados.csv"
```

No hace falta decidir todos los candidatos para exportar: `apply` conserva los
pendientes y solo elimina los aprobados. La interfaz también muestra comandos
con rutas absolutas para aplicar y reanudar. Si se guardan nuevas decisiones,
avisa que hay que reexportar. No se debe editar ni reexportar el CSV usado por una
aplicación interrumpida que se quiera retomar: `apply --resume` exige exactamente
el mismo CSV. Para aplicar una revisión posterior hay que iniciar una aplicación
nueva, con otra carpeta de salida o con `--overwrite` deliberadamente.

La interfaz solo prepara decisiones: no ejecuta `apply` ni borra fragmentos.
El servidor se limita a `127.0.0.1` en estos comandos; no publica el corpus en red.

Pruebas de persistencia, CSV completo y compatibilidad con aplicación:

```powershell
python -m pytest -vv tests/test_revision_ruido.py
```

Las pruebas de interfaz requieren Streamlit. Para ejecutarlas también:

```powershell
.\.venv-revision\Scripts\python.exe -m pip install pytest
.\.venv-revision\Scripts\python.exe -m pytest -vv tests/test_revision_ruido.py
```

## Arquitectura

`ruido_core.py` contiene métricas puras, once clases detectoras y la
consolidación de spans. `3_Eliminar_ruido.py` contiene la lectura JSONL estricta,
el índice SQLite, checkpoints, CSV, muestreo y aplicación de decisiones.

La auditoría tiene cuatro fases:

1. Indexa por bloques en SQLite la frecuencia por documento, origen y posición.
2. Calcula el SHA-256 completo de la entrada.
3. Ejecuta los detectores registro por registro y escribe todos los candidatos.
4. Recorre el CSV en streaming y crea una muestra estratificada, determinista y
   sin reemplazo.

Los detectores disponibles son `NAVIGATION_RESIDUAL`, `COOKIE_NOTICE`,
`REPEATED_HEADER_FOOTER`, `LINK_LIST_WITHOUT_CONTENT`, `ADVERTISING`,
`HTTP_ERROR_PAGE`, `REPEATED_CHARACTERS`, `SCRAPED_CODE`,
`NEAR_EMPTY_DOCUMENT`, `AUTOMATIC_INDEX` y
`EXCESSIVE_INTERNAL_REPETITION`. Todos combinan señales; una palabra aislada o
un único umbral general no produce por sí solo una propuesta.

## Consolidación y trazabilidad

Dos propuestas total o parcialmente superpuestas se fusionan si su acción es
la misma. El span resultante es el intervalo mínimo que cubre ambas; conserva
todos los códigos, nombres y métricas. Una propuesta de eliminar el registro y
otra de eliminar un fragmento permanecen separadas porque sus acciones no son
equivalentes.

Cada `candidate_id` incorpora la versión de auditoría, el SHA-256 de entrada, la
identidad estable del registro, motivos, offsets y hash exacto del fragmento. El
resumen guarda además un acumulador conmutativo del conjunto completo de IDs.
Así, `apply` rechaza un CSV con candidatos omitidos, agregados o duplicados,
aunque las filas hayan sido reordenadas en Excel o LibreOffice.

Antes de aplicar una decisión se verifican de nuevo el SHA-256 de entrada, el
hash del registro, la identidad estable, los offsets, el texto exacto, el hash
del fragmento y el propio `candidate_id`. No se buscan coincidencias aproximadas
en otra posición. Los spans aprobados que se superponen se unen y se eliminan
una sola vez; las primeras apariciones de repeticiones internas no se proponen.

## Archivos adicionales de estado

Además de los archivos solicitados, se crean dos bases SQLite:

- `auditoria_global.sqlite3`: frecuencias de bloques para encabezados y pies.
- `aplicacion_decisiones.sqlite3`: CSV revisado indexado por registro.

Son estado persistente para procesar corpus grandes sin cargar bloques o
decisiones completos en memoria y forman parte de la reanudación segura.

Durante la detección, las estadísticas globales de SQLite se precargan por lote
en lugar de consultarse bloque por bloque. `--sqlite-cache-mb` controla la caché
de páginas y `--sqlite-mmap-mb` el máximo de lectura mapeada. Estos parámetros
son de rendimiento y pueden cambiarse al reanudar sin invalidar el checkpoint.

Este paso no implementa privacidad, filtro de idioma, Onion, MinHash ni
deduplicación entre documentos. Esas responsabilidades pertenecen a otras
etapas del pipeline.
