# Plan del pipeline de datos en Hugging Face

Esta guía define la organización privada del corpus, su versionado, la ejecución del paso 4 de deduplicación y la transición futura hasta el Dataset público y el fine-tuning.

## 1. Principios de organización

- GitHub conserva el código y su historial.
- El bucket privado conserva datos, auditorías, manifiestos y checkpoints activos.
- La estructura del bucket refleja la numeración de `Base de datos/` y `Procesamiento/`.
- Cada salida se guarda bajo una versión inmutable; no se sobrescriben versiones publicadas.
- Los benchmarks externos se almacenan como referencias, no como resultados del corpus.
- El fine-tuning consume las particiones finales, pero sus checkpoints viven en otro bucket.
- Las versiones remotas antiguas solo se eliminan después de descargarlas y verificarlas localmente.
- El Dataset repo público contiene únicamente la versión final autorizada.

## 2. Secuencia completa

```text
0 - Crudo
1 - Unificado
2 - Normalizacion
3 - Eliminar ruido
4 - Dedup interna
5 - Anonimizacion
6 - Dedup benchmark
7 - Particiones
8 - Publicacion
```

El fine-tuning es un consumidor del paso 7, no una transformación anterior:

```text
7 - Particiones
       ├── 8 - Publicacion del Dataset
       └── Fine-tuning y evaluacion
```

## 3. Buckets y repositorios

### Bucket privado de datos

Nombre sugerido: `corpus-biomedico-DAP`.

```text
Base de datos/
├── 0 - Crudo/
├── 1 - Unificado/v1.0.0/
├── 2 - Normalizacion/v1.0.0/
├── 3 - Eliminar ruido/v1.0.0/
├── 4 - Dedup interna/v1.0.0/
├── 5 - Anonimizacion/v1.0.0/
├── 6 - Dedup benchmark/v1.0.0/
└── 7 - Particiones/v1.0.0/
Referencias/
└── Benchmarks/
Catalogo/
├── versiones.json
└── CURRENT.json
```

Cada etapa versionada utiliza:

```text
v1.0.0/
├── data/           # Corpus promovido como salida oficial
├── runs/           # Pilotos y ejecuciones todavía no promovidas
├── auditoria/      # CSV, muestras, decisiones y resúmenes
├── checkpoints/    # Solo el checkpoint activo necesario
├── metadata/
│   └── manifest.json
└── _SUCCESS        # Se crea únicamente tras validar la etapa
```

### Bucket privado de entrenamiento

Nombre sugerido: `corpus-biomedico-training`.

```text
experiments/
└── modelo-base/
    └── experimento-001/
        ├── config/
        ├── checkpoints/
        ├── logs/
        ├── metrics/
        └── manifest.json
evaluations/
```

Separarlo evita que checkpoints grandes de modelos afecten la retención o los permisos del corpus.

### Dataset público

La salida validada de `7 - Particiones/vX.Y.Z/data/` se publica en un Dataset repo, preferentemente como Parquet fragmentado:

```text
README.md
data/
├── train-00000-of-000NN.parquet
└── validation-00000-of-000NN.parquet
```

Nunca se publican datos crudos, checkpoints, candidatos pendientes ni versiones anteriores no autorizadas.

## 4. Versionado y trazabilidad

- `PATCH` (`v1.0.1`): corrección sin cambio sustancial del criterio.
- `MINOR` (`v1.1.0`): nuevas fuentes o ampliación del corpus.
- `MAJOR` (`v2.0.0`): cambio importante de criterio, esquema o composición.

No usar carpetas de datos llamadas `latest`. `Catalogo/CURRENT.json` será un puntero pequeño a la versión vigente.

Cada `manifest.json` debe registrar, como mínimo:

```json
{
  "pipeline_step": 4,
  "step_name": "Dedup interna",
  "dataset_version": "v1.0.0",
  "run_id": "20261004T180000Z-a1b2c3d",
  "status": "completed",
  "input_uri": "Base de datos/3 - Eliminar ruido/v1.0.0/data/sin_ruido.jsonl",
  "input_sha256": "...",
  "output_uri": "Base de datos/4 - Dedup interna/v1.0.0/data/deduplicado.jsonl",
  "output_sha256": "...",
  "git_commit": "a1b2c3d",
  "algorithm_version": "dedup-v1",
  "parameters": {},
  "job_id": "...",
  "hardware": "cpu-upgrade"
}
```

El paso siguiente solo debe consumir una versión con `manifest.json` válido y `_SUCCESS`.

## 5. Estado actual de dedup

- Entrada: `Base de datos/3 - Eliminar ruido/sin_ruido.jsonl` (23,84 GB).
- Checkpoint: `Base de datos/4 - Dedup interna/dedup_estado.sqlite3` (806 MB).
- Journal SQLite: aproximadamente 118 MB.
- Último checkpoint confirmado: 623.920 registros, todavía en indexación.

El checkpoint histórico conserva ruta absoluta y `mtime`, pero el código actualizado compara una identidad portable basada en tamaño y muestra hash. Puede reanudarse en Linux desde una copia consistente de SQLite. El archivo local se conserva intacto como respaldo; nunca se debe subir una base activa junto con su journal.

## 6. Preparar la cuenta

1. Abrir **Settings > Billing**.
2. Añadir un método de pago y cargar inicialmente **USD 10**.
3. Abrir **Settings > Access Tokens**.
4. Crear `procesamiento-local`.
5. Preferir un token `fine-grained` para administrar Jobs y leer/escribir buckets. Temporalmente puede usarse un token `write`.

No guardar el token en scripts, `.env`, documentación ni Git. No se necesita GPU ni obligatoriamente una suscripción PRO. Ver precios actuales en <https://huggingface.co/docs/hub/en/jobs-pricing>.

## 7. Instalar y autenticar la CLI

```powershell
py -m venv .venv-hf
.\.venv-hf\Scripts\python.exe -m pip install --upgrade pip huggingface_hub
$hfCli = ".\.venv-hf\Scripts\hf.exe"
& $hfCli --version
& $hfCli auth login
& $hfCli auth whoami
```

Agregar `.venv-hf/` al `.gitignore`.

### Variables del proyecto

Estas variables no son secretos. PowerShell las olvida al cerrar la terminal, por lo que se debe pegar este bloque después de activar `.venv-hf` en cada terminal nueva:

```powershell
$hfCli = ".\.venv-hf\Scripts\hf.exe"
$hfNamespace = "anna-bianca"
$bucketName = "corpus-biomedico-DAP"
$datasetVersion = "v1.0.0"
$inputFile = ".\Base de datos\3 - Eliminar ruido\sin_ruido.jsonl"
$inputHash = "B5C8C88CFB0930A6FBEB4AC6C912446642BE75C7CA5859E8C6E7B18BC050D81F"
$remoteStep3 = "hf://buckets/$hfNamespace/$bucketName/Base de datos/3 - Eliminar ruido/$datasetVersion/data"
```

El token de Hugging Face no se agrega a este bloque ni se guarda en el repositorio.

## 8. Probar Jobs y crear el bucket

```powershell
& $hfCli jobs run `
  --name prueba-conexion `
  --flavor cpu-basic `
  --timeout 5m `
  python:3.12 `
  -- python -c "print('Hugging Face Jobs funcionando')"
```

No continuar hasta obtener `COMPLETED`.

```powershell
& $hfCli buckets create "$hfNamespace/$bucketName" --private
& $hfCli buckets info "$hfNamespace/$bucketName"
```

Confirmar en la web que figure como **Private**. Documentación: <https://huggingface.co/docs/hub/storage-buckets>.

## 9. Subir la entrada del paso 4

El SHA-256 calculado para la entrada actual es:

```text
B5C8C88CFB0930A6FBEB4AC6C912446642BE75C7CA5859E8C6E7B18BC050D81F
```

Está guardado localmente en `Base de datos/3 - Eliminar ruido/sin_ruido.sha256.txt`. Para recalcularlo con progreso visible:

```powershell
python ".\tools\sha256_with_progress.py" ".\Base de datos\3 - Eliminar ruido\sin_ruido.jsonl" --output ".\Base de datos\3 - Eliminar ruido\sin_ruido.sha256.txt"
```

Previsualizar y ejecutar la subida:

```powershell
& $hfCli buckets sync `
  ".\Base de datos\3 - Eliminar ruido" `
  $remoteStep3 `
  --include "sin_ruido.jsonl" `
  --dry-run

& $hfCli buckets sync `
  ".\Base de datos\3 - Eliminar ruido" `
  $remoteStep3 `
  --include "sin_ruido.jsonl"

& $hfCli buckets list `
  "$hfNamespace/$bucketName/Base de datos/3 - Eliminar ruido/$datasetVersion/data" `
  -R -h
```

El plan previo debe incluir únicamente `sin_ruido.jsonl`. Si se interrumpe, repetir el comando; la sincronización es incremental.

## 10. Piloto remoto único de 50.000 registros

El caso de 500 registros ya fue validado localmente con el adaptador de Hugging Face. Para evitar dos Jobs remotos, se hará un solo piloto de 50.000 registros con `cpu-upgrade`, un máximo de seis horas y sin reintento automático.

Primero previsualizar el comando sin enviar el Job:

```powershell
$submitDedup = ".\Procesamiento\4 - Dedup interna\hf\submit_dedup.ps1"

powershell.exe -ExecutionPolicy Bypass -File $submitDedup `
  -Namespace $hfNamespace `
  -RunId "piloto-50000" `
  -MaxRecords 50000 `
  -Timeout "6h" `
  -DryRun
```

Después de comprobar las rutas impresas, repetir sin `-DryRun`:

```powershell
powershell.exe -ExecutionPolicy Bypass -File $submitDedup `
  -Namespace $hfNamespace `
  -RunId "piloto-50000" `
  -MaxRecords 50000 `
  -Timeout "6h" `
  -Attempts 1
```

El launcher usa lotes de 2.000 registros, `max-bucket=64`, checkpoints cada cinco minutos y `--resume`. Guarda resultados en `runs/piloto-50000/` y el único checkpoint activo en `checkpoints/piloto-50000/`.
### Registro de Jobs del piloto

- `anna-bianca/6ac2e9e9fbc85ba6823a3918` — envío con configuración inválida (`image --attempts`) porque la CLI 1.33.0 no admite `--attempts`; no cuenta como piloto válido y debe cancelarse.
- `anna-bianca/6ac2eac7fbc85ba6823a3960` — configuración correcta, pero falló antes de procesar datos porque `4_Dedup_interna.py` asumía una profundidad fija de carpetas incompatible con el montaje `/app`. Corregido y cubierto por prueba. URL: <https://huggingface.co/jobs/anna-bianca/6ac2eac7fbc85ba6823a3960>.
- `anna-bianca/6ac2ebfafbc85ba6823a39c0` — piloto remoto de 50.000 registros completado correctamente el 4 de octubre de 2026 con `cpu-upgrade`: 475 s de ejecución, 18.287 candidatos, 2.245 pares verificados y 1.037 grupos. Checkpoint SQLite de 71.491.584 bytes validado con `quick_check=ok`, fase `exported`. URL: <https://huggingface.co/jobs/anna-bianca/6ac2ebfafbc85ba6823a39c0>.
## 11. Seguimiento y control

El comando de envío imprime el ID del Job. Usarlo en:

```powershell
& $hfCli jobs ls
& $hfCli jobs logs -f ID_DEL_JOB
& $hfCli jobs inspect ID_DEL_JOB
& $hfCli jobs stats ID_DEL_JOB
& $hfCli jobs wait ID_DEL_JOB
& $hfCli jobs cancel ID_DEL_JOB
```

`Ctrl+C` durante `logs -f` deja de seguir los logs, pero no detiene el Job. Para detener el cómputo hay que ejecutar `jobs cancel` explícitamente.

Comprobar resultados y checkpoint:

```powershell
& $hfCli buckets list `
  "$hfNamespace/$bucketName/Base de datos/4 - Dedup interna/v1.0.0/runs/piloto-50000" `
  -R -h

& $hfCli buckets list `
  "$hfNamespace/$bucketName/Base de datos/4 - Dedup interna/v1.0.0/checkpoints/piloto-50000" `
  -R -h
```
## 12. Validación del piloto único

El piloto se considera válido cuando:

- el Job termina en `COMPLETED`;
- `runs/piloto-50000/run.json` indica `completed`;
- existen `grupos.csv`, `miembros.csv`, `acciones_propuestas.csv` y `auditoria_resumen.json`;
- existen `checkpoints/piloto-50000/latest.sqlite3` y `latest.json`;
- `latest.json` informa `phase: exported` e `indexed_count: 50000`;
- duración, costo, memoria, pares candidatos/verificados y grupos resultan razonables.

No se enviará una segunda ejecución del piloto. La restauración está cubierta por pruebas locales automatizadas; remotamente se comprobará que `latest.sqlite3` sea legible y que `latest.json` marque la fase final. Si la auditoría completa se interrumpe, se reanudará con el mismo `RunId` y parámetros.
## 13. Elegir hardware

Comenzar con `cpu-upgrade`: 8 vCPU, 32 GB de RAM y 50 GB temporales. Considerar `cpu-xl` —16 vCPU, 124 GB y 1 TB temporal— solo si el piloto demuestra que hace falta. El script no usa GPU.

```powershell
& $hfCli jobs hardware
```

## 14. Auditoría completa

Solo lanzarla después de validar el piloto remoto único:

```powershell
powershell.exe -ExecutionPolicy Bypass -File $submitDedup `
  -Namespace $hfNamespace `
  -RunId "completo-v1" `
  -Timeout "7d" `
```

La CLI instalada no ofrece reintentos automáticos. Como el checkpoint queda en el bucket y el launcher incluye `--resume`, un nuevo envío manual con el mismo `RunId` puede continuar desde el último snapshot confirmado. Si siete días no alcanzan, lanzar otro Job con el mismo `RunId`, entrada y parámetros, aumentando `Timeout`.

Después de revisar `grupos.csv`, la aplicación de decisiones se enviará con `-Mode apply` y la ruta remota de `grupos_revisados.csv`. Antes de considerar el resultado oficial se debe validar `deduplicado.jsonl` y promoverlo desde `runs/completo-v1/` hacia `data/`, generando `metadata/manifest.json` y `_SUCCESS`.
### Registro de la auditoría completa

- Job ID: `anna-bianca/6ac2f323404719ba37651bba`
- Nombre: `dedup-audit-completo-v1`
- Estado inicial: enviado
- Hardware: `cpu-upgrade`
- Timeout máximo: `7d`
- Entrada: `Base de datos/3 - Eliminar ruido/v1.0.0/data/sin_ruido.jsonl`
- Resultados: `Base de datos/4 - Dedup interna/v1.0.0/runs/completo-v1/`
- Checkpoint: `Base de datos/4 - Dedup interna/v1.0.0/checkpoints/completo-v1/`
- URL: <https://huggingface.co/jobs/anna-bianca/6ac2f323404719ba37651bba>
## 15. Descargar y verificar resultados

```powershell
& $hfCli jobs inspect ID_DEL_JOB
& $hfCli buckets list `
  "$hfNamespace/$bucketName/Base de datos/4 - Dedup interna/v1.0.0/runs/completo-v1" `
  -R -h
```

Descargar sin borrar nada:

```powershell
$archiveRoot = "D:\Archivo corpus"
$remoteRun = "hf://buckets/$hfNamespace/$bucketName/Base de datos/4 - Dedup interna/v1.0.0/runs/completo-v1"
$localRun = Join-Path $archiveRoot "Base de datos\4 - Dedup interna\v1.0.0\runs\completo-v1"

& $hfCli buckets sync $remoteRun $localRun --dry-run
& $hfCli buckets sync $remoteRun $localRun
```

No usar `--delete` al descargar. `D:\Archivo corpus` es un ejemplo: elegir una unidad con espacio suficiente y fuera del repositorio Git.

## 16. Política obligatoria antes de borrar versiones remotas

Sí, las versiones antiguas pueden descargarse y conservarse en la computadora. Ninguna eliminación remota debe ser automática. Para cada versión o piloto se seguirá este procedimiento:

### 16.1. Congelar la versión

- Confirmar que no haya un Job escribiendo en esa ruta.
- Confirmar que el Job haya terminado.
- Guardar el Job ID, commit de Git, parámetros, manifiesto y resumen.

### 16.2. Previsualizar y descargar

```powershell
$remoteArchive = "hf://buckets/$hfNamespace/$bucketName/Base de datos/4 - Dedup interna/v1.0.0/runs/piloto-50000"
$localArchive = "D:\Archivo corpus\Base de datos\4 - Dedup interna\v1.0.0\runs\piloto-50000"

& $hfCli buckets sync $remoteArchive $localArchive --dry-run
& $hfCli buckets sync $remoteArchive $localArchive
```

### 16.3. Crear inventario y hashes locales

```powershell
Get-ChildItem -LiteralPath $localArchive -Recurse -File |
  Sort-Object FullName |
  Select-Object FullName,Length,LastWriteTimeUtc |
  Export-Csv -LiteralPath (Join-Path $localArchive "ARCHIVE_INVENTORY.csv") `
    -NoTypeInformation -Encoding UTF8

Get-ChildItem -LiteralPath $localArchive -Recurse -File |
  Where-Object Name -ne "SHA256SUMS.csv" |
  Get-FileHash -Algorithm SHA256 |
  Select-Object Path,Hash |
  Export-Csv -LiteralPath (Join-Path $localArchive "SHA256SUMS.csv") `
    -NoTypeInformation -Encoding UTF8
```

### 16.4. Verificar antes de autorizar el borrado

- Comparar el tamaño total y la cantidad de archivos con `hf buckets list -R -h`.
- Abrir y validar los JSON de resumen y manifiesto.
- Confirmar que ningún archivo tenga tamaño cero inesperado.
- Guardar `ARCHIVE_INVENTORY.csv` y `SHA256SUMS.csv`.
- Para datos irremplazables, copiar el archivo local a un segundo disco antes de borrar la nube.

La regla será:

```text
Descargado + inventariado + hashes generados + verificado
                         ↓
                 apto para borrar
```

### 16.5. Borrado remoto manual

Solo después de una confirmación explícita y una segunda revisión se podrá usar:

```powershell
& $hfCli buckets rm `
  "$hfNamespace/$bucketName/Base de datos/4 - Dedup interna/v1.0.0/runs/piloto-50000" `
  --recursive
```

Este comando es destructivo y la eliminación del bucket no tiene historial recuperable. Antes se debe volver a listar la ruta exacta. Nunca borrar una versión marcada por `Catalogo/CURRENT.json`, una salida todavía consumida por el paso siguiente ni el único checkpoint de un Job activo.

## 17. Política de retención

### Mantener temporalmente en la nube

- La entrada y la salida de la etapa que esté ejecutándose.
- La salida oficial que todavía sea necesaria como entrada del paso siguiente.
- Un solo checkpoint activo por ejecución en curso.
- Manifiestos, resúmenes y auditorías pequeños mientras se valida el pipeline.

### Mantener al finalizar el pipeline

- La versión final anonimizada, descontaminada y dividida.
- El Dataset público correspondiente a esa versión.
- Opcionalmente, una copia privada de la versión final.
- `Catalogo/`, manifiestos y resúmenes, que ocupan muy poco espacio.

### Descargar y luego retirar de la nube

- Fuentes crudas cuando ya no sean necesarias para un Job y exista archivo local verificado.
- Pilotos terminados que ya cumplieron su función.
- Versiones reemplazadas por una versión validada más nueva.
- Checkpoints de ejecuciones completadas o abandonadas.
- Intermedios reconstruibles que ya no sean entrada activa.

### No borrar automáticamente

- Una versión sin archivo local verificado.
- La versión `CURRENT`.
- Datos sujetos a revisión o aprobación pendiente.
- La entrada o el checkpoint de un Job activo.
- La única copia existente de datos crudos o resultados irremplazables.

Dado que cada corpus completo puede ocupar 20–25 GB, conservar todas las etapas y versiones superaría rápidamente la cuota privada. Durante el procesamiento se mantiene en la nube solo lo operacional; al finalizar, los intermedios y crudos se archivan localmente y la nube conserva principalmente la versión final, el Dataset publicado y metadatos pequeños.

## 18. Etapas posteriores

### Paso 5: anonimización

Entrada: `4 - Dedup interna/vX.Y.Z/data/deduplicado.jsonl`.
Salida: `5 - Anonimizacion/vX.Y.Z/data/anonimizado.jsonl`.
Debe incluir auditoría de falsos positivos, categorías removidas y manifiesto.

### Paso 6: deduplicación contra benchmarks

Los benchmarks se guardan en `Referencias/Benchmarks/<nombre>/<version>/`. El manifiesto registra exactamente cuáles se usaron. La salida es `6 - Dedup benchmark/vX.Y.Z/data/sin_contaminacion.jsonl`.

### Paso 7: particiones

Genera `train` y `validation` de manera determinista, con semilla registrada, evitando que grupos relacionados queden repartidos entre particiones.

### Paso 8: publicación

Publica solamente la versión aprobada en un Dataset repo con Dataset Card, licencias, fuentes, proceso de anonimización, deduplicación, benchmarks, estadísticas y limitaciones. El tag público debe coincidir con `vX.Y.Z`.

### Fine-tuning

Consume una versión congelada del paso 7. Sus checkpoints, configuraciones, métricas y evaluaciones se almacenan en `corpus-biomedico-training`, no en el bucket de datos ni en el Dataset público.

## 19. Código local y Hugging Face

GitHub continúa como fuente oficial:

```powershell
python -m pytest -v
git status --short
git add Procesamiento tests
git commit -m "Describe el cambio realizado"
git push
```

La arquitectura prevista es un único motor de procesamiento con adaptadores local y Hugging Face. No se debe mantener una copia independiente del algoritmo que pueda producir resultados distintos.

## 20. Migración opcional del checkpoint local

La compatibilidad portable ya está implementada. Para conservar los 623.920 registros actuales:

1. Confirmar que ningún proceso escriba el checkpoint.
2. Respaldar juntos SQLite y el journal.
3. Recuperar el journal y crear una copia consistente con `sqlite3.Connection.backup()`.
4. Calcular el SHA-256 completo de la entrada.
5. Usar el código actualizado, cuya identidad no depende de ruta ni `mtime`.
6. Probar una reanudación desde otra ruta.
7. Subir la copia consistente como `checkpoints/<run-id>/latest.sqlite3`, nunca archivos activos.
8. Mantener el mismo `--max-bucket` y `--max-records`.

No editar manualmente la tabla `meta`.

## 21. Checklist

### Cuenta e infraestructura

- [ ] Método de pago y USD 10 configurados.
- [ ] Token registrado con `hf auth login`.
- [ ] Bucket de datos confirmado como privado.
- [ ] Bucket de entrenamiento separado antes del fine-tuning.
- [ ] Job de conexión en `COMPLETED`.

### Datos y versionado

- [ ] Versión `vX.Y.Z` definida.
- [ ] Hash de entrada guardado.
- [ ] Manifiesto y `_SUCCESS` requeridos para promover una etapa.
- [ ] `CURRENT.json` apunta a una versión existente y validada.
- [ ] Benchmarks guardados en `Referencias/`.

### Dedup

- [ ] Checkpoint local preservado.
- [x] Piloto remoto único de 50.000 completado y validado.
- [ ] Auditoría completa ejecutada con timeout explícito.
- [ ] Resultado revisado y promovido a `data/`.

### Archivo y eliminación

- [ ] Job detenido antes de archivar.
- [ ] Descarga local completada sin `--delete`.
- [ ] Cantidad y tamaño de archivos comparados.
- [ ] `ARCHIVE_INVENTORY.csv` generado.
- [ ] `SHA256SUMS.csv` generado.
- [ ] JSON de resumen y manifiesto validados.
- [ ] Segunda copia local realizada si el contenido es irremplazable.
- [ ] Confirmación explícita antes de cualquier borrado remoto.
- [ ] Ruta remota exacta listada nuevamente antes de eliminar.


