$ErrorActionPreference = 'Stop'
$root = '.'
$log  = Join-Path $root 'tools\deploy-ui-update.log'
function Log($m) {
  $line = "[{0}] {1}" -f (Get-Date -Format 'HH:mm:ss'), $m
  Add-Content -Path $log -Value $line -Encoding UTF8
}
Set-Content -Path $log -Value '' -Encoding UTF8

try {
  Log "deploy start (service workbuddy-gateway)"

  $nssm = Join-Path $root 'tools\nssm.exe'
  $exe  = Join-Path $root 'workbuddy-gateway.exe'
  $new  = Join-Path $root 'workbuddy-gateway.new.exe'
  if (-not (Test-Path $new)) { throw "new binary missing: $new" }

  Log "stopping service"
  & $nssm stop workbuddy-gateway | Out-Null

  $stopped = $false
  for ($i = 0; $i -lt 40; $i++) {
    $svc = Get-Service -Name workbuddy-gateway -ErrorAction SilentlyContinue
    if ($null -eq $svc) { throw "service not found" }
    if ($svc.Status -eq 'Stopped') { $stopped = $true; break }
    Start-Sleep -Milliseconds 500
  }
  if (-not $stopped) { throw "service did not stop in 20s, status: $($svc.Status)" }
  Log "service stopped"

  $proc = Get-Process -Name 'workbuddy-gateway' -ErrorAction SilentlyContinue
  if ($proc) {
    Log "waiting for process exit"
    $proc | Wait-Process -Timeout 20 -ErrorAction SilentlyContinue
  }
  if (Get-Process -Name 'workbuddy-gateway' -ErrorAction SilentlyContinue) {
    throw "process still running, cannot replace binary"
  }

  $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
  $backup = "$exe.$stamp.bak"
  Copy-Item -Path $exe -Destination $backup -Force
  Log "old binary backed up: $backup"

  Copy-Item -Path $new -Destination $exe -Force
  Log "new binary in place"

  Log "starting service"
  & $nssm start workbuddy-gateway | Out-Null

  for ($i = 0; $i -lt 40; $i++) {
    $svc = Get-Service -Name workbuddy-gateway
    if ($svc.Status -eq 'Running') { break }
    Start-Sleep -Milliseconds 500
  }
  Log "service status: $($svc.Status)"

  Start-Sleep -Seconds 2
  try {
    $ui = Invoke-WebRequest 'http://127.0.0.1:8316/ui/' -UseBasicParsing -TimeoutSec 10
    Log "web ui http $($ui.StatusCode)"
  } catch {
    Log "web ui check failed: $($_.Exception.Message)"
  }
  try {
    $js = (Invoke-WebRequest 'http://127.0.0.1:8316/ui/app.js' -UseBasicParsing -TimeoutSec 10).Content
    if ($js -match 'unblock-model' -and $js -match 'data-copy-model') {
      Log "frontend asset check passed (unblock + copy present)"
    } else {
      Log "warning: new frontend logic not found in served asset"
    }
  } catch {
    Log "frontend asset check failed: $($_.Exception.Message)"
  }

  Log "deploy done"
} catch {
  Log "deploy failed: $($_.Exception.Message)"
  try {
    $svc = Get-Service -Name workbuddy-gateway -ErrorAction SilentlyContinue
    if ($svc -and $svc.Status -ne 'Running') {
      Log "trying to restart service"
      & (Join-Path $root 'tools\nssm.exe') start workbuddy-gateway | Out-Null
    }
  } catch { Log "fallback start failed: $($_.Exception.Message)" }
  exit 1
}
