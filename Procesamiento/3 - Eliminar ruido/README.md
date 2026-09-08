# Paso 3: detección y eliminación auditable de ruido

Este paso separa deliberadamente la detección heurística de la eliminación. El
modo `audit` nunca crea un corpus limpio ni modifica registros: produce un CSV
completo para revisión humana. El modo `apply` vuelve a leer el JSONL original y
solo elimina candidatos cuya columna `decision` sea exactamente `eliminar`.

## Uso

```powershell
python "Procesamiento\3 - Eliminar ruido\3_Eliminar_ruido.py" --mode audit
```

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

Este paso no implementa privacidad, filtro de idioma, Onion, MinHash ni
deduplicación entre documentos. Esas responsabilidades pertenecen a otras
etapas del pipeline.
