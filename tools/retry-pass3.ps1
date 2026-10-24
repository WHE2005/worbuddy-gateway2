$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Net.Http

$base = 'http://127.0.0.1:8327/v1'
$apiKey = 'REDACTED'
$targets = @(
  'deepseek-v3.1','deepseek-v3-0324','deepseek-v4-pro-exclusive',
  'glm-4.6','glm-4.6v','glm-4.7','glm-5.0','glm-5.0-turbo',
  'kimi-k2-thinking','minimax-m2.5','gpt-5.3-codex'
)

$handler = New-Object System.Net.Http.HttpClientHandler
$client = New-Object System.Net.Http.HttpClient($handler)
$client.Timeout = [TimeSpan]::FromSeconds(150)
$client.DefaultRequestHeaders.Authorization = New-Object System.Net.Http.Headers.AuthenticationHeaderValue('Bearer', $apiKey)

$results = @()
foreach ($m in $targets) {
    $payload = @{ model = $m; messages = @(@{ role = 'user'; content = 'hi' }); max_tokens = 64; stream = $false }
    if ($m -eq 'glm-5.0-turbo') { $payload.reasoning_effort = 'low' }
    $body = $payload | ConvertTo-Json -Depth 6 -Compress

    $status = 0; $text = ''; $ms = 0
    $content = New-Object System.Net.Http.StringContent($body, [System.Text.Encoding]::UTF8, 'application/json')
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $resp = $client.PostAsync("$base/chat/completions", $content).GetAwaiter().GetResult()
        $sw.Stop()
        $bytes = $resp.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        $text = [System.Text.Encoding]::UTF8.GetString($bytes)
        $status = [int]$resp.StatusCode
    } catch {
        $sw.Stop(); $status = -1; $text = $_.Exception.Message
    }
    $ms = [int]$sw.ElapsedMilliseconds

    $err = ''; $reply = ''
    try {
        $jo = $text | ConvertFrom-Json
        if ($jo.error) { $err = [string]$jo.error.message } elseif ($jo.choices) { $reply = [string]$jo.choices[0].message.content }
    } catch { $err = $text }

    $results += [pscustomobject]@{ model=$m; http=$status; ms=$ms; reply=($reply -replace '\s+',' ').Trim(); error=($err -replace '\s+',' ').Trim() }
    Write-Host ("{0} http={1} {2}ms" -f $m, $status, $ms)
    Start-Sleep -Seconds 20
}

$results | ConvertTo-Json -Depth 5 | Set-Content -Path '.\tools\model-probe-pass3.json' -Encoding UTF8
Write-Host "DONE"
