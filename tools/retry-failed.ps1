$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Net.Http

$base = 'http://127.0.0.1:8327/v1'
$apiKey = 'REDACTED'
$failed = @(
  'deepseek-v3.1','deepseek-v3-0324','deepseek-v4-pro-exclusive',
  'gemini-3.5-flash','glm-4.6','glm-4.6v','glm-4.7','glm-5.0','glm-5.0-turbo',
  'gpt-5.3-codex','gpt-5.4','gpt-5.5','gpt-5.6-luna','gpt-5.6-sol','gpt-5.6-terra',
  'kimi-k2-thinking','minimax-m2.5','primary-model'
)

$handler = New-Object System.Net.Http.HttpClientHandler
$client = New-Object System.Net.Http.HttpClient($handler)
$client.Timeout = [TimeSpan]::FromSeconds(120)
$client.DefaultRequestHeaders.Authorization = New-Object System.Net.Http.Headers.AuthenticationHeaderValue('Bearer', $apiKey)

$outJson = '.\tools\model-probe-failed.json'
$results = @()

foreach ($m in $failed) {
    $body = @{ model = $m; messages = @(@{ role = 'user'; content = 'hi' }); max_tokens = 8; stream = $false } | ConvertTo-Json -Depth 6 -Compress
    $finalStatus = 0; $finalBody = ''; $ms = 0; $attempts = 0
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        $attempts = $attempt
        $content = New-Object System.Net.Http.StringContent($body, [System.Text.Encoding]::UTF8, 'application/json')
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            $resp = $client.PostAsync("$base/chat/completions", $content).GetAwaiter().GetResult()
            $sw.Stop()
            $bytes = $resp.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
            $text = [System.Text.Encoding]::UTF8.GetString($bytes)
            $finalStatus = [int]$resp.StatusCode
            $finalBody = $text
            if ($finalStatus -eq 200) { break }
            if ($finalStatus -eq 429 -and $attempt -lt 3) { Start-Sleep -Seconds 15; continue }
            break
        }
        catch {
            $sw.Stop()
            $finalStatus = -1
            $finalBody = $_.Exception.Message
            break
        }
        finally { $ms = [int]$sw.ElapsedMilliseconds }
    }
    $err = ''
    $reply = ''
    try {
        $jo = $finalBody | ConvertFrom-Json
        if ($jo.error) { $err = [string]$jo.error.message; if ($jo.error.code) { $err = "code=" + $jo.error.code + " " + $err } }
        elseif ($jo.choices) { $reply = [string]$jo.choices[0].message.content }
    } catch { $err = $finalBody }
    $results += [pscustomobject]@{ model = $m; http = $finalStatus; attempts = $attempts; ms = $ms; reply = ($reply -replace '\s+',' ').Trim(); error = ($err -replace '\s+',' ').Trim() }
    Write-Host ("{0} http={1} attempts={2}" -f $m, $finalStatus, $attempts)
    Start-Sleep -Seconds 8
}

$results | ConvertTo-Json -Depth 5 | Set-Content -Path $outJson -Encoding UTF8
Write-Host "DONE"
