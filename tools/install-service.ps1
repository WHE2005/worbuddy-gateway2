$ErrorActionPreference = 'Stop'
$root  = '.'
$nssm  = Join-Path $root 'tools\nssm.exe'
$exe   = Join-Path $root 'workbuddy-gateway.exe'
$logs  = Join-Path $root 'logs'
$svc   = 'workbuddy-gateway'
$out   = Join-Path $root 'tools\install-result.txt'

function Log($m) { Add-Content -Path $out -Value ("[" + (Get-Date -Format 'HH:mm:ss') + "] " + $m) }

try {
    Set-Content -Path $out -Value "install start"

    New-Item -ItemType Directory -Force -Path $logs | Out-Null

    Log "stopping running instances"
    Get-Process -Name workbuddy-gateway -ErrorAction SilentlyContinue | Stop-Process -Force
    Start-Sleep -Seconds 2

    $existing = Get-Service -Name $svc -ErrorAction SilentlyContinue
    if ($existing) {
        Log "existing service found, removing"
        & $nssm stop $svc | Out-Null
        Start-Sleep -Seconds 1
        & $nssm remove $svc confirm | Out-Null
        Start-Sleep -Seconds 2
    }

    Log "installing service"
    & $nssm install $svc $exe
    if ($LASTEXITCODE -ne 0) { throw "nssm install failed: $LASTEXITCODE" }

    & $nssm set $svc AppDirectory $root
    & $nssm set $svc AppParameters 'serve -webui'
    & $nssm set $svc DisplayName 'WorkBuddy Local Gateway'
    & $nssm set $svc Description 'CodeBuddy / WorkBuddy account pool as OpenAI + Anthropic compatible local API gateway'
    & $nssm set $svc Start SERVICE_AUTO_START
    & $nssm set $svc AppStdout (Join-Path $logs 'service-stdout.log')
    & $nssm set $svc AppStderr (Join-Path $logs 'service-stderr.log')
    & $nssm set $svc AppRotateFiles 1
    & $nssm set $svc AppRotateOnline 1
    & $nssm set $svc AppRotateBytes 10485760
    & $nssm set $svc AppExit Default Restart
    & $nssm set $svc AppRestartDelay 5000
    & $nssm set $svc AppThrottle 1500
    & $nssm set $svc AppStopMethodConsole 5000
    try { & $nssm set $svc AppNoConsole 1 } catch { Log "AppNoConsole skipped" }
    try { & $nssm set $svc ObjectName LocalSystem } catch { Log "ObjectName skipped" }

    Log "starting service"
    Start-Service $svc
    Start-Sleep -Seconds 6

    $s = Get-Service -Name $svc
    Log ("service status=" + $s.Status + " startType=" + $s.StartType)
    Log "DONE"
}
catch {
    Log ("ERROR: " + $_.Exception.Message)
    Log "FAILED"
}
