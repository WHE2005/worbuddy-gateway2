# WorkBuddy 网关一键升级脚本
# 用法：  .\tools\update.ps1              # 构建 + 测试 + 提权切换（UAC 弹窗）
#         .\tools\update.ps1 -SkipTests  # 跳过测试
#         .\tools\update.ps1 -SkipBuild  # 只切换已有 workbuddy-gateway.new.exe
# 流程：构建 -> 测试 -> 提权(停服务->备份->换二进制->起服务) -> 健康检查 -> 失败自动回滚
param(
    [switch]$SkipBuild,
    [switch]$SkipTests
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$log  = Join-Path $root 'tools\update-result.txt'

function Log($m) { Add-Content -Path $log -Value ("[" + (Get-Date -Format 'HH:mm:ss') + "] " + $m) }

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    # 构建与测试不需要管理员；提权会话只做服务切换
    if (-not $SkipBuild) {
        Log "building..."
        Push-Location $root
        try {
            go build -trimpath -ldflags "-s -w" -o workbuddy-gateway.new.exe .
            if ($LASTEXITCODE -ne 0) { throw "go build failed: $LASTEXITCODE" }
        } finally { Pop-Location }
        Log "build OK"
    }
    if (-not $SkipTests) {
        Log "running tests..."
        Push-Location $root
        try {
            go test -count=1 .
            if ($LASTEXITCODE -ne 0) { throw "go test failed: $LASTEXITCODE" }
        } finally { Pop-Location }
        Log "tests OK"
    }
    Log "elevating for service switch (UAC)..."
    Start-Process powershell -Verb RunAs -Wait -ArgumentList @(
        '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $PSCommandPath,
        '-SkipBuild', '-SkipTests'
    )
    exit
}

# ---------------- 管理员段：切换 + 健康检查 + 回滚 ----------------
$svc   = 'workbuddy-gateway'
$nssm  = Join-Path $root 'tools\nssm.exe'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$backup = Join-Path $root "archive\exe-backups\workbuddy-gateway.exe.$stamp.bak"

# 从 config.json 读 API 端口做健康检查（允许命令行 -port 覆盖的场景自行调整）
$apiPort = 8317
try {
    $cfg = Get-Content (Join-Path $root 'config.json') -Raw | ConvertFrom-Json
    if ($cfg.gateway.apiPort) { $apiPort = [int]$cfg.gateway.apiPort }
} catch { Log "WARN: 读 config.json 失败，健康检查用默认端口 8317" }

function Get-Health {
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$apiPort/health" -UseBasicParsing -TimeoutSec 3
        return ($resp.StatusCode -eq 200)
    } catch { return $false }
}

Set-Content -Path $log -Value "[$stamp] update start (apiPort=$apiPort)"
$rolledBack = $false
try {
    Copy-Item (Join-Path $root 'workbuddy-gateway.exe') $backup -Force
    Log "backup: $backup"

    & $nssm stop $svc | Out-Null
    Start-Sleep -Seconds 2
    Copy-Item (Join-Path $root 'workbuddy-gateway.new.exe') (Join-Path $root 'workbuddy-gateway.exe') -Force
    Log "binary swapped"
    & $nssm start $svc | Out-Null

    # 健康检查：最多等 20 秒
    $healthy = $false
    for ($i = 0; $i -lt 10; $i++) {
        Start-Sleep -Seconds 2
        if (Get-Health) { $healthy = $true; break }
        Log "health check $i/10 failed, retrying..."
    }
    if (-not $healthy) { throw "health check failed after switch" }

    $status = & $nssm status $svc
    Log "service status: $status; health: OK"
    Log "DONE"
} catch {
    Log ("ERROR: " + $_.Exception.Message)
    if (-not $rolledBack) {
        Log "rolling back to $backup ..."
        & $nssm stop $svc | Out-Null
        Start-Sleep -Seconds 2
        Copy-Item $backup (Join-Path $root 'workbuddy-gateway.exe') -Force
        & $nssm start $svc | Out-Null
        Start-Sleep -Seconds 4
        if (Get-Health) { Log "rollback OK, service healthy on previous binary" }
        else { Log "rollback performed but health check still failing; check logs/" }
    }
    exit 1
}
