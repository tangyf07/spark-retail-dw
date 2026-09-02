$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
if (-not $env:SPARK_LOCAL_IP) { $env:SPARK_LOCAL_IP = "127.0.0.1" }

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    Write-Error "Java 17+ is required for PySpark. Install a JDK and retry."
}

$Python = if ($env:PYTHON) { $env:PYTHON } else { "python" }
& $Python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install -U pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
& .\.venv\Scripts\python.exe jobs\pipeline.py
