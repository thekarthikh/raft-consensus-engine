$ErrorActionPreference = 'Stop'
# Select the highest-term leader reported by a currently running service.
# This reads historical logs, not a live leadership RPC. During elections, wait
# for a stable leader; the client still rejects followers and does not retry writes.
$taskRunning = @(docker compose ps --services --status running)
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect Compose services' }
$taskLogs = docker compose logs --no-color
if ($LASTEXITCODE -ne 0) { throw 'Cannot read Compose logs' }
$taskCandidates = @($taskLogs | ForEach-Object {
    if ($_ -match 'Node (node[123]) BECAME LEADER for term (\d+)') {
        if ($taskRunning -contains $Matches[1]) {
            [pscustomobject]@{ Node = $Matches[1]; Term = [long]$Matches[2] }
        }
    }
})
if ($taskCandidates.Count -eq 0) { throw 'No running service has reported leadership yet; wait and retry' }
($taskCandidates | Sort-Object Term -Descending | Select-Object -First 1).Node
