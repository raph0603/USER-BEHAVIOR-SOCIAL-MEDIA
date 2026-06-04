param(
    [switch]$Kafka,
    [switch]$Spark,
    [switch]$SparkKafka,
    [switch]$Lakehouse,
    [switch]$Clean
)

$runKafka = $Kafka -or (-not $Spark -and -not $SparkKafka)
$runSpark = $Spark -or (-not $Kafka -and -not $SparkKafka)
$runSparkKafka = $SparkKafka -or (-not $Kafka -and -not $Spark)
$runLakehouse = $Lakehouse
$logDir = ".\tests\logs"
$runId = Get-Date -Format "yyyyMMdd-HHmmss"
$mainLog = Join-Path $logDir "test_stack_$runId.log"
$sparkTestTopic = "spark-test-topic"
$pipelineFailed = $false

function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string]$LogPath
    )

    $output = cmd /c "$Command 2>&1"
    $exitCode = $LASTEXITCODE
    if ($output) {
        $output | Tee-Object -FilePath $LogPath -Append | Out-Null
    }

    return $exitCode
}

function Write-Section {
    param([Parameter(Mandatory = $true)][string]$Title)

    $line = ('=' * 72)
    Write-Host "\n$line"
    Write-Host $Title
    Write-Host $line
}

function Set-PipelineFailed {
    param([Parameter(Mandatory = $true)][string]$Message)

    $script:pipelineFailed = $true
    Write-Host $Message
}

function Show-Progress {
    param(
        [Parameter(Mandatory = $true)][string]$Activity,
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)][int]$Percent
    )

    Write-Progress -Activity $Activity -Status $Status -PercentComplete $Percent
}

function Invoke-DockerExec {
    param(
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Command
    )

    docker exec $Container /bin/bash -lc $Command
}

function Kill-SparkJobs {
    Write-Host "Step: kill lingering Spark submit processes"
    Invoke-DockerExec -Container "spark-master" -Command "pkill -9 -f spark-submit || true"
    Invoke-DockerExec -Container "spark-master" -Command "pkill -9 -f kafka_stream_test.py || true; pkill -9 -f kafka_to_iceberg_bronze.py || true; pkill -9 -f bronze_to_silver.py || true; pkill -9 -f lakehouse_check.py || true"
    Invoke-DockerExec -Container "spark-worker-1" -Command "pkill -9 -f spark-submit || true"
    Invoke-DockerExec -Container "spark-worker-2" -Command "pkill -9 -f spark-submit || true"
    Invoke-DockerExec -Container "spark-master" -Command "rm -f /opt/spark/tests/streaming/kafka_stream_test.pid /opt/spark/tests/lakehouse/bronze_stream.pid"
}

function Wait-Ready {
    Write-Section "Readiness check"

    Write-Host "Step: wait for kafka"
    for ($i = 1; $i -le 12; $i++) {
        $ok = docker exec kafka /bin/bash -lc "/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092 >/dev/null 2>&1"; if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep -Seconds 5
    }

    Write-Host "Step: wait for schema registry"
    for ($i = 1; $i -le 12; $i++) {
        $srUp = docker ps --filter "name=schema-registry" --filter "status=running" --format "{{.Names}}"
        if ($srUp -like "*schema-registry*") {
            $curlCheck = docker exec schema-registry /bin/bash -lc "command -v curl >/dev/null 2>&1 && curl -fsS http://schema-registry:8081/subjects >/dev/null 2>&1"
            if ($LASTEXITCODE -eq 0) { break }
            $curlMissing = docker exec schema-registry /bin/bash -lc "command -v curl >/dev/null 2>&1"
            if ($LASTEXITCODE -ne 0) { break }
        }
        Start-Sleep -Seconds 5
    }

    Write-Host "Step: wait for minio"
    for ($i = 1; $i -le 12; $i++) {
        $ok = docker exec minio /bin/sh -c "curl -fsS http://minio:9000/minio/health/ready >/dev/null 2>&1"; if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep -Seconds 5
    }

    Write-Host "Step: wait for spark master"
    for ($i = 1; $i -le 12; $i++) {
        $ok = docker exec spark-master /bin/bash -lc "nc -z spark-master 7077 >/dev/null 2>&1"; if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep -Seconds 5
    }

}

Write-Section "Starting services"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
if ($Clean) {
    Remove-Item -Recurse -Force .\data\minio -ErrorAction SilentlyContinue
}
docker compose up -d minio minio-init kafka schema-registry kafdrop spark-master spark-worker-1 spark-worker-2 | Out-Null
Start-Transcript -Path $mainLog -Append | Out-Null
Write-Host "Run log: $mainLog"

Wait-Ready

Write-Section "Cleanup previous Spark jobs"
Write-Host "Step: wait for spark-master"
for ($i = 1; $i -le 12; $i++) {
    $sparkUp = docker ps --filter "name=spark-master" --filter "status=running" --format "{{.Names}}"
    if ($sparkUp -eq "spark-master") {
        break
    }
    Start-Sleep -Seconds 5
}
Kill-SparkJobs

if ($runKafka) {
    Write-Section "Kafka test"
    Write-Host "Step: create/list topic"
    $kafkaLog = Join-Path $logDir "kafka_$runId.log"
    $topicCreateExit = Invoke-LoggedCommand "docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic test-topic --partitions 2 --replication-factor 1 --bootstrap-server kafka:9092" $kafkaLog
    $topicListExit = Invoke-LoggedCommand "docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092" $kafkaLog
    if ($topicCreateExit -ne 0 -or $topicListExit -ne 0) {
        Set-PipelineFailed "Kafka test: FAILED"
    }
    Write-Host "Log: $kafkaLog"
    Write-Host "Kafdrop: http://localhost:9002"
}

if ($runSpark) {
    Write-Section "Spark test"
    Write-Host "Step: SparkPi"
    $sparkLog = Join-Path $logDir "sparkpi_$runId.log"
    $sparkExit = Invoke-LoggedCommand "docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.2.jar 10" $sparkLog
    if ($sparkExit -ne 0) {
        Set-PipelineFailed "Spark test: FAILED"
    }
    Write-Host "Log: $sparkLog"
    Write-Host "Spark UI: http://localhost:8080"
}

if ($runSparkKafka) {
    Write-Section "Spark + Kafka test"
    Write-Host "Step: create isolated Spark/Kafka topic"
    $sparkKafkaLog = Join-Path $logDir "spark_kafka_$runId.log"
    $sparkKafkaTopicExit = Invoke-LoggedCommand "docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $sparkTestTopic --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092" $sparkKafkaLog
    if ($sparkKafkaTopicExit -ne 0) {
        Set-PipelineFailed "Spark + Kafka topic setup: FAILED"
    }

    Write-Host "Step: start streaming job"
    docker exec spark-master /bin/bash -lc 'mkdir -p /opt/spark/tests/streaming && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=1 --conf spark.executor.cores=1 /opt/spark/tests/streaming/kafka_stream_test.py > /opt/spark/tests/streaming/kafka_stream_test.log 2>&1 & echo $! > /opt/spark/tests/streaming/kafka_stream_test.pid'

    $message = "test-" + (Get-Date -Format "yyyyMMdd-HHmmss")
    docker exec kafka /bin/bash -lc "printf '%s\\n' '$message' | timeout 5s /opt/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic $sparkTestTopic"

    Write-Host "Waiting for stream log..."
    $streamReady = $false
    for ($i = 1; $i -le 12; $i++) {
        $logSize = docker exec spark-master /bin/bash -lc "if [[ -f /opt/spark/tests/streaming/kafka_stream_test.log ]]; then wc -c < /opt/spark/tests/streaming/kafka_stream_test.log; else echo 0; fi"
        if ([int]$logSize -gt 0) {
            $streamReady = $true
            break
        }
        Show-Progress -Activity "Spark + Kafka" -Status "Waiting for stream log" -Percent ($i * 100 / 12)
        Start-Sleep -Seconds 5
    }
    Show-Progress -Activity "Spark + Kafka" -Status "Waiting for stream log" -Percent 100

    if ($streamReady) {
        Write-Host "Stream log (tail):"
        Invoke-LoggedCommand 'docker exec spark-master /bin/bash -lc "tail -n 50 /opt/spark/tests/streaming/kafka_stream_test.log"' $sparkKafkaLog | Out-Null
        Write-Host "Log: $sparkKafkaLog"
    } else {
        Write-Host "Warning: stream log stayed empty after waiting."
        Write-Host "Log: $sparkKafkaLog"
        Set-PipelineFailed "Spark + Kafka test: FAILED"
    }

    Write-Host "Step: stop streaming job"
    Invoke-DockerExec -Container "spark-master" -Command "if [[ -f /opt/spark/tests/streaming/kafka_stream_test.pid ]]; then cat /opt/spark/tests/streaming/kafka_stream_test.pid | xargs -r kill -9 || true; fi"
    Invoke-DockerExec -Container "spark-master" -Command "pkill -9 -f kafka_stream_test.py || true"
    Invoke-DockerExec -Container "spark-worker-1" -Command "pkill -9 -f kafka_stream_test.py || true"
    Invoke-DockerExec -Container "spark-worker-2" -Command "pkill -9 -f kafka_stream_test.py || true"
}

Write-Section "Producer check"
Write-Host "Step: run bounded synthetic producer"
docker compose run --rm -e PRODUCER_MODE=synthetic -e PRODUCER_MAX_EVENTS=20 -e EVENT_INTERVAL_SEC=0 playwright-producer | Out-Null
if ($LASTEXITCODE -ne 0) {
    Set-PipelineFailed "Producer run: FAILED"
}
Write-Host "Step: verify producer emits at least one message"
$producerOk = $false
for ($i = 1; $i -le 6; $i++) {
    $probe = docker exec kafka /bin/bash -lc "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic --from-beginning --max-messages 1 --timeout-ms 3000"
    if ($probe) {
        $producerOk = $true
        break
    }
    Start-Sleep -Seconds 2
}
if (-not $producerOk) {
    Write-Host "Producer check: FAILED (no messages in topic)"
}

if ($producerOk) {
    Write-Host "Producer check: OK"
} else {
    Set-PipelineFailed "Producer check: FAILED (mock did not emit)"
}

if ($runLakehouse) {
    Write-Section "Lakehouse test"
    if (-not $producerOk) {
        Write-Host "Skipping Lakehouse: producer did not emit messages."
        $pipelineFailed = $true
    } else {
    Write-Host "Step: ensure MinIO bucket"
    for ($i = 0; $i -lt 10; $i++) {
        docker compose run --rm minio-init | Out-Null
        if ($LASTEXITCODE -eq 0) {
            break
        }
        Start-Sleep -Seconds 2
    }

    Write-Host "Step: start Bronze streaming job"
    docker exec spark-master /bin/bash -lc 'mkdir -p /opt/spark/tests/lakehouse && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/streaming/kafka_to_iceberg_bronze.py > /opt/spark/tests/lakehouse/bronze_stream.log 2>&1 & echo $! > /opt/spark/tests/lakehouse/bronze_stream.pid'

    Write-Host "Waiting for Bronze data..."

    Write-Host "Step: check Bronze"
    $bronzeOk = $false
    $bronzeDeadline = (Get-Date).AddSeconds(60)
    $bronzeAttempt = 0
    while ((Get-Date) -lt $bronzeDeadline) {
        $bronzeAttempt++
        $bronzeLog = Join-Path $logDir "lakehouse_bronze_$runId.log"
        Show-Progress -Activity "Lakehouse" -Status "Checking Bronze (attempt $bronzeAttempt)" -Percent 0
        $bronzeCheckExit = Invoke-LoggedCommand 'docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/tests/lakehouse/lakehouse_check.py lakehouse.bronze.events 1"' $bronzeLog
        if ($bronzeCheckExit -eq 0) {
            Show-Progress -Activity "Lakehouse" -Status "Bronze check OK" -Percent 100
            $bronzeOk = $true
            Write-Host "Bronze check: OK"
            break
        }
        Start-Sleep -Seconds 2
    }
    Write-Host "Log: $bronzeLog"

    if (-not $bronzeOk) {
        Set-PipelineFailed "Bronze check: FAILED"
    }

    if ($bronzeOk) {
        Write-Host "Step: run Silver batch"
        $silverLog = Join-Path $logDir "lakehouse_silver_$runId.log"
        $silverBatchExit = Invoke-LoggedCommand 'docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/jobs/batch/bronze_to_silver.py"' $silverLog
        if ($silverBatchExit -ne 0) {
            Set-PipelineFailed "Silver batch: FAILED"
        }
        Write-Host "Log: $silverLog"
    } else {
        Write-Host "Skipping Silver: Bronze check failed."
    }

    if ($bronzeOk) {
        Write-Host "Step: check Silver"
        $silverOk = $false
        $silverDeadline = (Get-Date).AddSeconds(60)
        $silverAttempt = 0
        while ((Get-Date) -lt $silverDeadline) {
            $silverAttempt++
            $silverLog = Join-Path $logDir "lakehouse_silver_$runId.log"
            Show-Progress -Activity "Lakehouse" -Status "Checking Silver (attempt $silverAttempt)" -Percent 0
            $silverCheckExit = Invoke-LoggedCommand 'docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.cores=1 /opt/spark/tests/lakehouse/lakehouse_check.py lakehouse.silver.events 1"' $silverLog
            if ($silverCheckExit -eq 0) {
                Show-Progress -Activity "Lakehouse" -Status "Silver check OK" -Percent 100
                $silverOk = $true
                Write-Host "Silver check: OK"
                break
            }
            Start-Sleep -Seconds 2
        }
        Write-Host "Log: $silverLog"
        if (-not $silverOk) {
            Set-PipelineFailed "Silver check: FAILED"
        }
    }

    Write-Host "Step: stop Bronze streaming job"
    Invoke-DockerExec -Container "spark-master" -Command "if [[ -f /opt/spark/tests/lakehouse/bronze_stream.pid ]]; then cat /opt/spark/tests/lakehouse/bronze_stream.pid | xargs -r kill -9 || true; fi"
    Invoke-DockerExec -Container "spark-master" -Command "pkill -9 -f kafka_to_iceberg_bronze.py || true"
    Invoke-DockerExec -Container "spark-worker-1" -Command "pkill -9 -f kafka_to_iceberg_bronze.py || true"
    Invoke-DockerExec -Container "spark-worker-2" -Command "pkill -9 -f kafka_to_iceberg_bronze.py || true"
    }
}

if ($Clean) {
    Write-Section "Cleanup"
    Write-Host "Step: save container logs"
    $composeLog = Join-Path $logDir "compose_$runId.log"
    docker compose logs --no-color | Out-File -FilePath $composeLog -Encoding utf8
    Write-Host "Log: $composeLog"
    Write-Host "Step: stop stack"
    docker compose down
    Remove-Item -Recurse -Force .\data\minio -ErrorAction SilentlyContinue
}

Stop-Transcript | Out-Null

if ($pipelineFailed) {
    exit 1
}

exit 0
