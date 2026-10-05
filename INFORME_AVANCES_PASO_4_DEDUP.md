# Paso 4 — Deduplicación interna y migración del procesamiento a Hugging Face

## Objetivo de la etapa

El cuarto paso del pipeline tiene como objetivo identificar contenido duplicado o parcialmente repetido dentro del corpus biomédico consolidado, sin eliminar información de manera automática. La entrada es `sin_ruido.jsonl`, resultado del paso 3, con un tamaño de 23.835.715.266 bytes (aproximadamente 23,84 GB) y alrededor de 2,39 millones de registros.

La deduplicación se diseñó como un proceso en dos momentos. Primero se ejecuta una auditoría que indexa el corpus, genera pares candidatos, verifica las coincidencias y produce grupos y acciones propuestas. Después se realiza una revisión humana de esos grupos. Solo las decisiones expresamente aprobadas se aplican en una ejecución posterior. Por lo tanto, la auditoría no modifica el archivo de entrada ni elimina registros.

## Criterio de deduplicación

La unidad de análisis es cada registro JSONL. El texto se tokeniza en palabras Unicode con unificación de mayúsculas y minúsculas, pero se conserva el texto original para las salidas y para cualquier eventual recorte.

La detección combina varios mecanismos:

1. Un hash del texto detecta copias exactas de al menos 20 palabras.
2. MinHash sobre secuencias de cinco palabras genera candidatos con similitud aproximada.
3. Anclas de pasajes de 16 palabras permiten detectar fragmentos compartidos entre documentos.
4. Los candidatos se verifican mediante similitud de Jaccard y alineación de pasajes idénticos. Para considerar un pasaje sustancial se exige una longitud mínima de 80 palabras.

La selección del registro a conservar utiliza como preferencia general `CoWeSe > SciELO > SPACCC > MMedC`. Esta prioridad no es absoluta: una versión sustancialmente más completa se conserva aunque provenga de una fuente de menor prioridad. Si dos registros contienen información propia, se conservan ambos y, cuando corresponde, solo se propone recortar del registro de menor prioridad el pasaje duplicado formado por oraciones completas.

El criterio es deliberadamente conservador. Una diferencia en cifras, dosis, negaciones o términos clínicos protegidos impide proponer la eliminación completa de una variante. Asimismo, los componentes excesivamente grandes y los buckets de candidatos muy frecuentes se restringen para evitar explosiones combinatorias y propuestas poco confiables.

## Implementación y capacidad de reanudación

El procesamiento utiliza SQLite como índice persistente. Allí se almacenan los registros indexados, sus huellas, los pares candidatos, las verificaciones, los grupos y el avance confirmado de cada fase. Las operaciones se confirman por lotes, lo que permite continuar desde el último checkpoint válido después de una interrupción.

La implementación distingue tres componentes:

- `dedup_core.py`, que contiene las reglas y funciones puras de comparación;
- `4_Dedup_interna.py`, que coordina el flujo, SQLite, la auditoría y la aplicación posterior de decisiones;
- `hf/dedup_hf.py`, que adapta el mismo flujo a Hugging Face Jobs sin duplicar el algoritmo.

Esta separación evita mantener una versión “local” y otra “cloud” con comportamientos diferentes. El mismo motor y los mismos parámetros producen los resultados en ambos entornos.

Para que un checkpoint pueda reanudarse en Windows o Linux, la identidad de la entrada dejó de depender de la ruta absoluta y de la fecha de modificación. En su lugar, se utiliza una identidad portable basada en el tamaño y una muestra hash del archivo. La aplicación final mantiene una verificación SHA-256 del contenido procesado.

## Motivos para dejar el procesamiento local

La primera ejecución local confirmó que el algoritmo funcionaba, pero también mostró las limitaciones operativas de continuar en la computadora de trabajo. El checkpoint local alcanzó 623.920 registros durante la indexación y su base SQLite llegó aproximadamente a 806 MB, acompañada por un journal de unos 118 MB. Completar el corpus exigía mantener la computadora encendida durante periodos prolongados y dejar disponibles de manera estable CPU, memoria, disco y la sesión de trabajo.

Los principales problemas de la ejecución exclusivamente local fueron:

- tiempos prolongados y dificultad para utilizar la computadora simultáneamente;
- mayor exposición a interrupciones por suspensión, reinicio, actualizaciones o cierre accidental de la terminal;
- dependencia de rutas absolutas y del sistema operativo para recuperar checkpoints antiguos;
- dificultad para observar el proceso y conservar un estado remoto recuperable;
- falta de aislamiento entre el trabajo cotidiano y un procesamiento intensivo de datos.

El cambio a Hugging Face no se planteó como una modificación del criterio científico, sino como una mejora de infraestructura. La lógica de deduplicación permanece sin cambios; se traslada la ejecución a un entorno más adecuado para procesos largos y reproducibles.

## Arquitectura en Hugging Face

Los datos se almacenan de forma privada en el bucket `corpus-biomedico-DAP`, siguiendo la misma numeración y nomenclatura del repositorio. La entrada del paso 4 se encuentra en:

```text
Base de datos/3 - Eliminar ruido/v1.0.0/data/sin_ruido.jsonl
```

El código continúa versionándose en Git y se monta en el Job como solo lectura. El bucket privado se monta con permisos de lectura y escritura. Los resultados del piloto se guardan en `runs/piloto-50000/`, mientras que el checkpoint activo se guarda separadamente en `checkpoints/piloto-50000/`.

SQLite no trabaja directamente sobre el bucket. La base activa permanece en el disco temporal rápido del Job y, cada cinco minutos, se crea una copia consistente mediante `sqlite3.Connection.backup()`. Esa copia se valida y se publica atómicamente como `latest.sqlite3`, acompañada por `latest.json`, que registra la fase y los contadores confirmados. Al iniciar un nuevo Job con el mismo identificador, el adaptador puede restaurar ese snapshot en el disco temporal y continuar.

Esta estrategia evita copiar una base SQLite mientras está siendo modificada, reduce las operaciones pequeñas sobre el almacenamiento remoto y mantiene un único checkpoint recuperable. Además, `run.json` registra el estado general de cada ejecución.

La elección inicial fue `cpu-upgrade`, ya que el algoritmo utiliza CPU y no obtiene una ventaja directa de una GPU. Cada Job tiene un timeout explícito para limitar el gasto máximo. Hugging Face factura el tiempo real durante el cual el Job está iniciando o ejecutándose, por lo que un timeout amplio funciona como techo de seguridad y no como una reserva que se cobra completa.

## Validación mediante un piloto remoto

Antes de procesar el corpus completo se realizaron pruebas unitarias y de integración local sobre portabilidad, restauración de checkpoints y consistencia de SQLite. Posteriormente se ejecutó un único piloto remoto de 50.000 registros para validar el flujo completo en Hugging Face.

El Job válido fue:

```text
anna-bianca/6ac2ebfafbc85ba6823a39c0
```

Sus resultados fueron los siguientes:

| Indicador | Resultado |
|---|---:|
| Estado final | `COMPLETED` |
| Hardware | `cpu-upgrade` |
| Tiempo de ejecución | 475 segundos (7 min 55 s) |
| Registros indexados | 50.000 |
| Pares candidatos | 18.287 |
| Pares verificados | 2.245 |
| Grupos detectados | 1.037 |
| Grupos excesivamente grandes | 0 |
| Buckets frecuentes omitidos | 1 |
| Tamaño del checkpoint final | 71.491.584 bytes |
| Fase final del checkpoint | `exported` |
| Integridad SQLite | `PRAGMA quick_check = ok` |

El único bucket frecuente omitido corresponde a la salvaguarda prevista para no generar cantidades combinatorias de pares a partir de una señal demasiado común. No representa un error del Job.

Se generaron correctamente `grupos.csv`, `miembros.csv`, `acciones_propuestas.csv`, `auditoria_resumen.json` y `run.json`. El resumen del piloto contiene un SHA-256 limitado al prefijo de 50.000 registros, por lo que es distinto del SHA-256 del archivo completo. El hash local del corpus completo es:

```text
B5C8C88CFB0930A6FBEB4AC6C912446642BE75C7CA5859E8C6E7B18BC050D81F
```

Durante el despliegue se detectaron y corrigieron dos problemas de infraestructura antes del piloto válido: una diferencia entre las opciones admitidas por la versión instalada de la CLI y la documentación más reciente, y una suposición sobre la profundidad de directorios que no era válida cuando el código se montaba directamente en `/app`. Ambos problemas ocurrieron antes de procesar datos, quedaron documentados y se cubrieron con validaciones adicionales.

## Evaluación de la decisión

La migración a Hugging Face resultó adecuada por las siguientes razones:

- separa el procesamiento intensivo del equipo de trabajo;
- mantiene los datos y resultados en un bucket privado;
- permite observar Jobs, logs, duración y estado desde la CLI o la interfaz web;
- conserva checkpoints consistentes y recuperables;
- permite fijar hardware, parámetros, versión del dataset e identificador de ejecución;
- cobra por tiempo efectivo de ejecución y permite limitarlo mediante timeout;
- conserva GitHub como fuente oficial del código y Hugging Face como infraestructura de datos y cómputo;
- facilita que las etapas posteriores reutilicen la misma organización versionada.

La solución también introduce responsabilidades nuevas: controlar costos, preservar la privacidad del bucket, evitar Jobs concurrentes con el mismo `run-id`, archivar localmente versiones antiguas antes de eliminarlas y verificar los manifiestos antes de promover una salida. Estas medidas quedaron incorporadas al plan operativo.

## Estado actual y próximos pasos

El piloto demuestra que el paso 4 puede ejecutarse correctamente en Hugging Face y que los resultados y checkpoints persisten en el bucket privado. No implica todavía que el corpus completo esté deduplicado.

El próximo avance será ejecutar la auditoría completa con un nuevo `run-id`, revisar sus métricas y descargar los CSV de auditoría. Posteriormente se realizará la revisión humana de los grupos y se ejecutará el modo `apply` únicamente con decisiones aprobadas. El resultado validado será promovido a la carpeta versionada `data/` y se registrará mediante un manifiesto y un marcador `_SUCCESS` antes de utilizarlo como entrada del paso 5 de anonimización.

## Referencias operativas

- [Hugging Face Jobs: configuración](https://huggingface.co/docs/hub/en/jobs-configuration)
- [Hugging Face Jobs: precios y facturación](https://huggingface.co/docs/hub/en/jobs-pricing)
- [Hugging Face Storage Buckets](https://huggingface.co/docs/hub/storage-buckets)