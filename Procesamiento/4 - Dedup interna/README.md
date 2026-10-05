# Paso 4: deduplicación interna

Entrada predeterminada: `Base de datos/3 - Eliminar ruido/sin_ruido.jsonl`.
Código: `Procesamiento/4 - Dedup interna/`.
Salida predeterminada: `Base de datos/4 - Dedup interna/`.

## Criterio para conservar contenido

La unidad es cada **registro JSONL**: CoWeSe contiene bloques; SciELO, documentos; SPACCC y MMedC, archivos. `doi` y `link` suelen identificar la colección, por lo que no se usan como prueba de que dos registros sean el mismo artículo.

1. Se comparan palabras Unicode con mayúsculas/minúsculas unificadas. No se cambia el texto original para encontrar coincidencias.
2. Un hash detecta copias exactas de al menos 20 palabras. MinHash en 5-gramas y anclas de pasajes de 16 palabras generan candidatos. Se verifican con Jaccard y pasajes idénticos de **80 palabras o más**.
3. Entre versiones comparables se prefiere **CoWeSe > SciELO > SPACCC > MMedC**. Una versión sustancialmente más completa gana aunque proceda de una fuente posterior. El umbral de contenido adicional es 80 palabras o el 10 % de la versión corta, lo que sea mayor.
4. Se propone eliminar un registro si coincide exactamente con otro o si casi todo su texto está contenido en una versión conservada. Una variante con cifras, dosis, negaciones o términos clínicos protegidos sin correspondencia no se propone como eliminación completa.
5. Si ambos registros contienen información propia, se conservan ambos. Se propone recortar del registro de menor prioridad únicamente oraciones completas dentro de pasajes idénticos. Una similitud aproximada por sí sola no autoriza un recorte.

El criterio es deliberadamente conservador y **toda acción queda pendiente hasta que se apruebe su grupo**. Los grupos grandes de anclas o MinHash (más de 64 registros por defecto) se omiten como generadores de pares para evitar explosiones combinatorias; el resumen informa cuántos se omitieron. Un componente relacionado de más de 500 registros solo recibe propuestas automáticas si todos son copias exactas; los demás quedan señalados para revisión sin propuesta de borrado. La detección aproximada no garantiza encontrar todas las copias y requiere revisar una muestra de falsos positivos y negativos.

## Auditoría

Desde la raíz del repositorio:

```powershell
python "Procesamiento\4 - Dedup interna\4_Dedup_interna.py" --mode audit
```

La auditoría emite un latido `[ACTIVO]` cada **15 segundos** durante todas las fases, incluso mientras SQLite está trabajando en un bloque largo. Muestra la fase, el avance en curso, el último checkpoint confirmado y el tiempo transcurrido. Para cambiar el intervalo:

```powershell
python "Procesamiento\4 - Dedup interna\4_Dedup_interna.py" --mode audit --heartbeat-seconds 5
```

`--heartbeat-seconds 0` desactiva los latidos. Los mensajes `[FASE]` indican cada cambio de etapa. Si la auditoría ya estaba corriendo antes de actualizar el código, detenerla con `Ctrl+C` y ejecutar el mismo comando con `--resume` para activar el latido; el avance confirmado se conserva.

La auditoría crea `grupos.csv`, `miembros.csv`, `acciones_propuestas.csv`, `auditoria_resumen.json` y `dedup_estado.sqlite3` en la carpeta de salida. `grupos.csv` tiene **una fila por grupo**. `miembros.csv` muestra cada registro y una vista previa; `acciones_propuestas.csv` muestra las eliminaciones y recortes con offsets y texto de muestra. Los grupos relacionados por una cadena de similitud no se eliminan automáticamente: cada eliminación se verifica contra un registro que se conserva.

Para un piloto sobre las primeras 500 líneas, usar una carpeta separada y nueva:

```powershell
python "Procesamiento\4 - Dedup interna\4_Dedup_interna.py" --mode audit `
  --max-records 500 `
  --output-dir "Base de datos\4 - Dedup interna\mi_piloto_500"
```

Si se interrumpe la indexación, generación de pares o verificación, repetir el mismo comando con `--resume`. Conservar el archivo de entrada y los parámetros `--max-records` y `--max-bucket`. La base SQLite confirma el avance por lotes. La auditoría no elimina nada de la entrada.

La indexación inserta registros y huellas en lotes de **1.000 registros** por defecto, con un límite adicional de 50.000 huellas por lote. `--batch-size 2000` reduce la frecuencia de los checkpoints si hay memoria suficiente; este parámetro se puede cambiar al reanudar. La verificación reutiliza cada pasaje ya alineado y compara n-gramas directamente, sin alterar el índice persistido. Una muestra de 500 registros bajó de 35,9 a 9,5 segundos en el mismo equipo; ese resultado no predice linealmente el tiempo del corpus completo.

## Revisión y aplicación

En una **copia** de `grupos.csv`, completar solamente:

- `decision`: `aprobar`, `rechazar` o vacío (pendiente).
- `keeper_override`: número de `record_no` de un miembro del mismo grupo si se quiere corregir el representante. Dejar vacío para usar el propuesto.

Las otras columnas identifican y validan el grupo; no deben editarse ni omitirse filas. Aprobar un grupo aplica todas las acciones que resulten de la selección del representante. Si se cambia `keeper_override`, la propuesta se recalcula desde las coincidencias verificadas. Revisar el grupo completo antes de aprobarlo.

```powershell
python "Procesamiento\4 - Dedup interna\4_Dedup_interna.py" --mode apply `
  --review-csv "Base de datos\4 - Dedup interna\grupos_revisados.csv"
```

La aplicación verifica el SHA-256 de la entrada y la integridad de todos los grupos. Produce `deduplicado.jsonl`, `acciones_aplicadas.csv` y `aplicacion_resumen.json`; añade `Paso 4: Dedup interna` a `procesamiento` de cada registro conservado. `sin_ruido.jsonl` permanece intacto. Para reanudar una aplicación interrumpida, usar `--resume` con **el mismo CSV de revisión, sin modificarlo**. Para una revisión posterior, empezar en otra carpeta de salida con una auditoría nueva.

## Verificación

```powershell
python -m unittest -v tests.test_dedup_interna
```

El piloto de 500 registros reales produjo 902 pares candidatos, 72 pares verificados y 21 grupos. Es un piloto del prefijo CoWeSe; no representa la tasa de duplicados del corpus completo. El corpus completo tiene aproximadamente 2,39 millones de registros y requiere mucho tiempo y espacio para el índice SQLite. Comprobar el espacio disponible y revisar el resumen antes de aprobar grupos.
## Ejecución en Hugging Face Jobs

La implementación remota reutiliza el mismo motor y las mismas reglas que la ejecución local. `hf/dedup_hf.py` mantiene `dedup_estado.sqlite3` en el disco temporal rápido del Job y crea snapshots SQLite consistentes en el bucket. No copia el archivo activo ni su journal.

Desde la raíz del repositorio, comprobar primero el comando sin enviar nada:

```powershell
powershell.exe -ExecutionPolicy Bypass -File ".\Procesamiento\4 - Dedup interna\hf\submit_dedup.ps1" `
  -Namespace "TU_USUARIO" `
  -RunId "piloto-500" `
  -MaxRecords 500 `
  -DryRun
```

Para enviar el piloto, quitar `-DryRun`. El launcher monta el código local como solo lectura, monta el bucket privado `corpus-biomedico-DAP`, usa `cpu-upgrade` y guarda:

- resultados en `Base de datos/4 - Dedup interna/v1.0.0/runs/<run-id>/`;
- el único checkpoint activo en `Base de datos/4 - Dedup interna/v1.0.0/checkpoints/<run-id>/latest.sqlite3`;
- estado del Job en `run.json` y metadatos del snapshot en `latest.json`.

Si el Job se interrumpe, ejecutar otra vez el mismo comando y `RunId`: `--resume` se incluye automáticamente. No ejecutar dos Jobs simultáneos con el mismo `RunId`.

Después de editar y subir al bucket una copia revisada de `grupos.csv`, aplicar las decisiones así:

```powershell
powershell.exe -ExecutionPolicy Bypass -File ".\Procesamiento\4 - Dedup interna\hf\submit_dedup.ps1" `
  -Namespace "TU_USUARIO" `
  -RunId "completo-v1" `
  -Mode apply `
  -ReviewCsv "/bucket/Base de datos/4 - Dedup interna/v1.0.0/runs/completo-v1/grupos_revisados.csv"
```

El checkpoint identifica la entrada por tamaño y una muestra hash, no por su ruta de Windows o Linux. Esto permite mover una copia idéntica y reanudarla; la aplicación final sigue verificando el SHA-256 completo de la entrada.
