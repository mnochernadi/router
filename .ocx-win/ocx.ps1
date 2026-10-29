# ocx.ps1 -- anonymous OpenCode CLI lane with automatic exit rotation.
#
# Always anonymous: own profile, own store, no credential ever. Nothing system-wide
# is touched: no WinINET proxy, no netsh, no PATH change. Isolation is per-process.
#
#   .\ocx.ps1 run "say OK"   completion; on the free-usage limit it moves to the next
#                            working exit and retries, refreshing the sub when dry
#   .\ocx.ps1 refresh        rebuild the exit pool from the subscriptions now
#   .\ocx.ps1 status         pool size, age, parked exits, credential state
#
# Isolation:
#   * own USERPROFILE + XDG_* under this folder -> own opencode.db, own auth store
#   * --standalone on every call -> never touches the desktop app's background service
#   * strip.py before every run -> any credential is deleted, the lane stays anonymous
#   * egress through the project's own xray on 127.0.0.1:18080, one exit per attempt
#   * pool rebuilt from the subs every 12 h and whenever it runs dry

param(
  [Parameter(Position = 0)][ValidateSet('run', 'refresh', 'status')][string]$Mode = 'status',
  [Parameter(Position = 1)][string]$Prompt = 'say OK',
  [int]$RelayPort = 18080,
  [int]$TimeoutSec = 180
)

$ErrorActionPreference = 'Stop'
$Root = $PSScriptRoot
$Cli = 'C:\Users\MONO\AppData\Local\Programs\@opencodedesktop\resources\opencode-cli.exe'
$Py = (Get-Command python).Source

if (-not (Test-Path $Cli)) { throw "opencode CLI not found at $Cli" }
foreach ($d in 'home', 'data', 'config', 'cache', 'proj') {
  New-Item -ItemType Directory -Force -Path (Join-Path $Root $d) | Out-Null
}

# ---- child environment: isolated profile + lane-local egress -------------------
$env:USERPROFILE     = Join-Path $Root 'home'
$env:HOME            = Join-Path $Root 'home'
$env:XDG_DATA_HOME   = Join-Path $Root 'data'
$env:XDG_CONFIG_HOME = Join-Path $Root 'config'
$env:XDG_CACHE_HOME  = Join-Path $Root 'cache'
$env:TMPDIR          = Join-Path $Root 'cache'
$env:HTTPS_PROXY     = "http://127.0.0.1:$RelayPort"
$env:HTTP_PROXY      = "http://127.0.0.1:$RelayPort"
$env:ALL_PROXY       = "http://127.0.0.1:$RelayPort"
$env:NO_PROXY        = '127.0.0.1,localhost'
$env:NO_COLOR        = '1'

switch ($Mode) {
  'status' {
    (& $Py (Join-Path $Root 'lane.py') status 2>&1).Trim()
    $a = Join-Path $Root 'cache\auth.txt'
    $p = Start-Process -FilePath $Cli -ArgumentList @('auth', 'list', '--standalone') `
      -WorkingDirectory (Join-Path $Root 'proj') -RedirectStandardOutput $a `
      -RedirectStandardError (Join-Path $Root 'cache\auth.err') -NoNewWindow -PassThru
    $p.WaitForExit(60000) | Out-Null
    $t = Get-Content $a -Raw -ErrorAction SilentlyContinue
    if ($t -match 'stored') {
      Write-Host "WARNING: lane carries a credential -- not anonymous." -ForegroundColor Yellow
    } else {
      Write-Host "anonymous lane: no credential, own bucket." -ForegroundColor Green
    }
  }
  'refresh' { & $Py (Join-Path $Root 'lane.py') refresh 2>&1 | ForEach-Object { $_ } }
  'run' {
    $s = & $Py (Join-Path $Root 'strip.py') 2>&1
    if ($s) { Write-Host $s.Trim() -ForegroundColor DarkGray }
    & $Py (Join-Path $Root 'lane.py') run $Prompt 2>&1 | ForEach-Object { $_ }
  }
}
