param([string]$Java = "java")
$ErrorActionPreference = "Stop"
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    & mvn -Pbenchmarks test-compile dependency:build-classpath '-Dmdep.includeScope=test' '-Dmdep.outputFile=target/benchmark-classpath.txt'
    if ($LASTEXITCODE -ne 0) { throw "Benchmark build failed" }
    $taskSeparator = [IO.Path]::PathSeparator
    $taskClasspath = "target/test-classes${taskSeparator}target/classes${taskSeparator}" + (Get-Content target/benchmark-classpath.txt -Raw).Trim()
    & $Java -cp $taskClasspath org.openjdk.jmh.Main 'com.raft.benchmark.RaftLogBenchmark.testLogAppend' -f 1 -t 1 -wi 2 -w 1s -i 5 -r 2s -jvmArgs '-Xms256m -Xmx256m' -rf json -rff target/wal-benchmark.json
    if ($LASTEXITCODE -ne 0) { throw "Benchmark run failed" }
} finally { Pop-Location }
