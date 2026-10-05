[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Namespace,

    [string]$Bucket = "corpus-biomedico-DAP",
    [string]$DatasetVersion = "v1.0.0",

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$RunId,

    [ValidateSet("audit", "apply")]
    [string]$Mode = "audit",
    [string]$ReviewCsv = "",

    [ValidateSet("cpu-basic", "cpu-upgrade", "cpu-xl", "cpu-performance")]
    [string]$Flavor = "cpu-upgrade",
    [string]$Timeout = "7d",
    [ValidateRange(0, [int]::MaxValue)]
    [int]$MaxRecords = 0,
    [ValidateRange(1, [int]::MaxValue)]
    [int]$BatchSize = 2000,
    [ValidateRange(2, [int]::MaxValue)]
    [int]$MaxBucket = 64,
    [ValidateRange(0, [double]::MaxValue)]
    [double]$CheckpointSeconds = 300,
    [string]$HfCli = ".\.venv-hf\Scripts\hf.exe",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$codePath = Join-Path $repoRoot "Procesamiento\4 - Dedup interna"
if (-not (Test-Path -LiteralPath $codePath -PathType Container)) {
    throw "No existe la carpeta de código: $codePath"
}
if (-not $DryRun -and
        -not (Test-Path -LiteralPath $HfCli -PathType Leaf) -and
        -not (Get-Command $HfCli -ErrorAction SilentlyContinue)) {
    throw "No se encontró la CLI de Hugging Face: $HfCli"
}

$bucketMount = "hf://buckets/$Namespace/${Bucket}:/bucket"
$inputFile = "/bucket/Base de datos/3 - Eliminar ruido/$DatasetVersion/data/sin_ruido.jsonl"
$stepRoot = "/bucket/Base de datos/4 - Dedup interna/$DatasetVersion"
$outputDir = "$stepRoot/runs/$RunId"
$checkpointDir = "$stepRoot/checkpoints/$RunId"
$jobName = "dedup-$Mode-$RunId"

$jobArgs = @(
    "jobs", "run",
    "--detach",
    "--name", $jobName,
    "--label", "pipeline=dedup",
    "--label", "mode=$Mode",
    "--label", ("dataset_version=" + $DatasetVersion.Replace('.', '_')),
    "--label", "run_id=$RunId",
    "--flavor", $Flavor,
    "--timeout", $Timeout,
    "-v", "${codePath}:/app:ro",
    "-v", $bucketMount,
    "python:3.12",
    "--",
    "python", "/app/hf/dedup_hf.py",
    "--mode", $Mode,
    "--input-file", $inputFile,
    "--output-dir", $outputDir,
    "--checkpoint-dir", $checkpointDir,
    "--scratch-dir", "/tmp/dedup-$RunId",
    "--checkpoint-seconds", $CheckpointSeconds.ToString([Globalization.CultureInfo]::InvariantCulture),
    "--batch-size", $BatchSize.ToString(),
    "--progress-every", "10000",
    "--heartbeat-seconds", "15",
    "--max-bucket", $MaxBucket.ToString(),
    "--resume"
)
if ($MaxRecords -gt 0) {
    $jobArgs += @("--max-records", $MaxRecords.ToString())
}

if ($ReviewCsv) {
    $jobArgs += @("--review-csv", $ReviewCsv)
}
if ($Mode -eq "apply" -and -not $ReviewCsv) {
    Write-Warning "No se indicó -ReviewCsv; se usará grupos.csv dentro de la salida."
}
Write-Host "Bucket:      $Namespace/$Bucket"
Write-Host "Entrada:     $inputFile"
Write-Host "Salida:      $outputDir"
Write-Host "Checkpoint:  $checkpointDir"
Write-Host "Modo:        $Mode"
Write-Host "Hardware:    $Flavor"
Write-Host "Timeout:     $Timeout"

if ($DryRun) {
    Write-Host "DRY RUN: no se enviará el Job."
    Write-Output ($HfCli + " " + (($jobArgs | ForEach-Object { '"' + $_ + '"' }) -join " "))
    exit 0
}

& $HfCli @jobArgs
if ($LASTEXITCODE -ne 0) {
    throw "hf jobs run terminó con código $LASTEXITCODE"
}
