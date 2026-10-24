$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Net.Http

$base = 'http://127.0.0.1:8327/v1'
$apiKey = 'REDACTED'
$targets = @('hy3','hy3-x','hy4-preview','hy4-preview-x','glm-5.1','glm-5.2','glm-5.3','glm-5.3-flash','glm-5v-turbo','auto','default')

$handler = New-Object System.Net.Http.HttpClientHandler
$client = New-Object System.Net.Http.HttpClient($handler)
$client.Timeout = [TimeSpan]::FromSeconds(120)
$client.DefaultRequestHeaders.Authorization = New-Object System.Net.Http.Headers.AuthenticationHeaderValue('Bearer', $apiKey)

$results = @()
$i = 0
foreach ($m in $targets) {
    $i++
    $body = @{ model = $m; messages = @(@{ role = 'user'; content = 'hi' }); max_tokens = 64; stream = $false } | ConvertTo-Json -Depth 6 -Compress
    $status = 0; $text = ''; $ms = 0
    $content = New-Object System.Net.Http.StringContent($body, [System.Text.Encoding]::UTF8, 'application/json')
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $resp = $client.PostAsync("$base/chat/completions", $content).GetAwaiter().GetResult()
        $sw.Stop()
        $bytes = $resp.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        $text = [System.Text.Encoding]::UTF8.GetString($bytes)
        $status = [int]$resp.StatusCode
    } catch { $sw.Stop(); $status = -1; $text = $_.Exception.Message }
    $ms = [int]$sw.ElapsedMilliseconds

    $err = ''; $reply = ''; $tokens = $null; $credit = $null
    try {
        $jo = $text | ConvertFrom-Json
        if ($jo.error) { $err = [string]$jo.error.message }
        elseif ($jo.choices) {
            $reply = [string]$jo.choices[0].message.content
            if ($jo.usage) { $tokens = $jo.usage.total_tokens; $credit = $jo.usage.credit }
        }
    } catch { $err = $text }

    $results += [pscustomobject]@{ model=$m; http=$status; ms=$ms; tokens=$tokens; credit=$credit; reply=($reply -replace '\s+',' ').Trim(); error=($err -replace '\s+',' ').Trim() }
    Write-Host ("[{0}/{1}] {2} http={3} {4}ms" -f $i, $targets.Count, $m, $status, $ms)
    Start-Sleep -Seconds 20
}

$results | ConvertTo-Json -Depth 5 | Set-Content -Path '.\tools\model-probe-new.json' -Encoding UTF8
Write-Host ("DONE ok=" + ($results | Where-Object { $_.http -eq 200 }).Count)
