$ErrorActionPreference = 'Stop'
$root = '.'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$log = "$root\tools\switch-result.txt"
Set-Content -Path $log -Value "[$stamp] switch start"
try {
    Copy-Item "$root\workbuddy-gateway.exe" "$root\archive\exe-backups\workbuddy-gateway.exe.$stamp.bak" -Force
    Add-Content $log "backup done: workbuddy-gateway.exe.$stamp.bak"
    & "$root\tools\nssm.exe" stop workbuddy-gateway | Out-Null
    Start-Sleep -Seconds 2
    Copy-Item "$root\workbuddy-gateway.new.exe" "$root\workbuddy-gateway.exe" -Force
    Add-Content $log "binary swapped"
    & "$root\tools\nssm.exe" start workbuddy-gateway | Out-Null
    Start-Sleep -Seconds 4
    $status = & "$root\tools\nssm.exe" status workbuddy-gateway
    Add-Content $log "service status: $status"
    Add-Content $log "DONE"
} catch {
    Add-Content $log ("ERROR: " + $_.Exception.Message)
    exit 1
}
