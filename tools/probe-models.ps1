$ErrorActionPreference = 'Stop'
$base = 'http://127.0.0.1:8327/v1'
$outCsv = '.\tools\model-probe.csv'
$outJson = '.\tools\model-probe.json'

$apiKey = 'REDACTED'
$headers = @{ Authorization = "Bearer $apiKey" }

$modelsResp = Invoke-RestMethod -Uri "$base/models" -Headers $headers -TimeoutSec 30
$models = $modelsResp.data | ForEach-Object { $_.id } | Sort-Object -Unique
$skip = @('hunyuan-image-alpha','hunyuan-image-alpha-edit','hunyuan-image-v3.0')
$targets = $models | Where-Object { $skip -notcontains $_ }

$results = @()
$i = 0
foreach ($m in $targets) {
    $i++
    $body = @{
        model = $m
        messages = @(@{ role = 'user'; content = 'hi' })
        max_tokens = 8
        stream = $false
    } | ConvertTo-Json -Depth 6 -Compress

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $status = 0; $err = ''; $content = ''; $credit = $null; $tokens = $null
    try {
        $resp = Invoke-WebRequest -Uri "$base/chat/completions" -Method Post -Body $body -ContentType 'application/json' -Headers $headers -TimeoutSec 90 -UseBasicParsing
        $status = [int]$resp.StatusCode
        $j = $resp.Content | ConvertFrom-Json
        $content = [string]$j.choices[0].message.content
        if ($null -ne $j.usage) {
            $credit = $j.usage.credit
            $tokens = $j.usage.total_tokens
        }
    }
    catch {
        $status = -1
        $err = $_.Exception.Message
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $raw = $sr.ReadToEnd()
            if ($raw) { $err = $raw }
        } catch {}
    }
    $sw.Stop()

    $results += [pscustomobject]@{
        model      = $m
        ok         = ($status -ge 200 -and $status -lt 300)
        http       = $status
        ms         = [int]$sw.ElapsedMilliseconds
        tokens     = $tokens
        credit     = $credit
        reply      = ($content -replace '\s+', ' ').Trim()
        error      = ($err -replace '\s+', ' ').Trim()
    }
    Write-Host ("[{0}/{1}] {2} http={3} {4}ms" -f $i, $targets.Count, $m, $status, [int]$sw.ElapsedMilliseconds)
    Start-Sleep -Milliseconds 1200
}

$results | Export-Csv -Path $outCsv -NoTypeInformation -Encoding UTF8
$results | ConvertTo-Json -Depth 5 | Set-Content -Path $outJson -Encoding UTF8
Write-Host ("DONE total=" + $results.Count + " ok=" + ($results | Where-Object ok).Count)
