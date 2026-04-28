param(
    [string]$From    = "auto",
    [string]$Account = "",
    [switch]$DryRun  = $false,
    [switch]$Resume  = $false
)

$ProjectDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$Script       = Join-Path $ProjectDir "run_backfill.py"
$LogDir       = Join-Path $ProjectDir "logs"
$LogFile      = Join-Path $LogDir "run_backfill.log"
$ProgressFile = Join-Path $ProjectDir "backfill_progress.json"

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}

Clear-Host
Write-Host ""
Write-Host "  SAADAA Meta Ads - Full Historical Backfill" -ForegroundColor Cyan
Write-Host "  Writing to: backfill_table" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Project  : $ProjectDir" -ForegroundColor Gray
Write-Host "  Script   : $Script" -ForegroundColor Gray
Write-Host "  Log file : $LogFile" -ForegroundColor Gray
Write-Host ""

if (-not (Test-Path (Join-Path $ProjectDir ".env"))) {
    Write-Host "  ERROR: .env not found at $ProjectDir" -ForegroundColor Red
    Write-Host "  Create it with META_ACCESS_TOKEN and SUPABASE_DB_URL" -ForegroundColor Red
    Read-Host "Press Enter to close"
    exit 1
}

if (-not (Test-Path $Script)) {
    Write-Host "  ERROR: full_backfill.py not found at $Script" -ForegroundColor Red
    Read-Host "Press Enter to close"
    exit 1
}

$PyArgs = @()
if ($From -and $From -ne "auto") {
    $PyArgs += "--from"
    $PyArgs += $From
}
if ($Account) {
    $PyArgs += "--account"
    $PyArgs += $Account
}
if ($DryRun) {
    $PyArgs += "--dry-run"
}

Write-Host "  Settings:" -ForegroundColor Yellow

if ($From -eq "auto") {
    Write-Host "    Start date : AUTO - detects earliest date from Meta API" -ForegroundColor Cyan
} else {
    Write-Host "    Start date : $From" -ForegroundColor White
}

if ($Account) {
    Write-Host "    Account    : $Account" -ForegroundColor White
} else {
    Write-Host "    Account    : All 3 accounts" -ForegroundColor White
}

if ($DryRun) {
    Write-Host "    Mode       : DRY RUN (no DB writes)" -ForegroundColor Yellow
} else {
    Write-Host "    Mode       : LIVE - writing to backfill_table" -ForegroundColor Green
}

Write-Host ""

if (Test-Path $ProgressFile) {
    Write-Host "  Progress file found - will resume from last checkpoint" -ForegroundColor Yellow
    try {
        $ProgressData = Get-Content $ProgressFile -Raw | ConvertFrom-Json
        $DoneCount = ($ProgressData.PSObject.Properties | Where-Object { $_.Value -eq "done" }).Count
        Write-Host "  $DoneCount chunks already completed" -ForegroundColor Gray
    } catch {
        Write-Host "  (could not read progress file)" -ForegroundColor Gray
    }
    Write-Host ""
}

if (-not $DryRun) {
    $confirm = Read-Host "  Start backfill? (y/N)"
    if ($confirm -notmatch '^[Yy]$') {
        Write-Host "  Cancelled." -ForegroundColor Gray
        exit 0
    }
}

Write-Host ""
Write-Host "  Starting - press Ctrl+C to pause (progress is saved)" -ForegroundColor Green
Write-Host ""

$StartTime = Get-Date
Set-Location $ProjectDir

try {
    & python $Script @PyArgs
    $ExitCode = $LASTEXITCODE
} catch {
    Write-Host "  ERROR running Python: $_" -ForegroundColor Red
    Read-Host "Press Enter to close"
    exit 1
}

$Elapsed    = (Get-Date) - $StartTime
$ElapsedStr = "{0}h {1}m {2}s" -f [int]$Elapsed.TotalHours, $Elapsed.Minutes, $Elapsed.Seconds

Write-Host ""

if ($ExitCode -eq 0) {
    Write-Host "  Backfill completed successfully" -ForegroundColor Green
} else {
    Write-Host "  Backfill finished with exit code $ExitCode" -ForegroundColor Yellow
    Write-Host "  Check log: $LogFile" -ForegroundColor Gray
}

Write-Host "  Time taken : $ElapsedStr" -ForegroundColor Cyan
Write-Host "  Log file   : $LogFile" -ForegroundColor Cyan
Write-Host ""

if (($ExitCode -eq 0) -and (-not $DryRun)) {
    Write-Host "  Data written to: backfill_table in Supabase" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Verify in Supabase SQL Editor:" -ForegroundColor Yellow
    Write-Host "    SELECT account_name, MIN(date), MAX(date), COUNT(*) FROM backfill_table GROUP BY account_name;" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  To keep primary_table fresh, start the cron runner:" -ForegroundColor Yellow
    Write-Host "    python cron_runner.py" -ForegroundColor Gray
    Write-Host ""
}

Read-Host "Press Enter to close"