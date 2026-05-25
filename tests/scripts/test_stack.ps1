param(
    [switch]$Kafka,
    [switch]$Spark,
    [switch]$SparkKafka
)

$runKafka = $Kafka -or (-not $Spark -and -not $SparkKafka)
$runSpark = $Spark -or (-not $Kafka -and -not $SparkKafka)
$runSparkKafka = $SparkKafka -or (-not $Kafka -and -not $Spark)

Write-Host "Starting services..."
docker-compose up -d | Out-Null

if ($runKafka) {
    Write-Host "Kafka test: create/list topic"
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic test-topic --partitions 2 --replication-factor 1 --bootstrap-server kafka:9092
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092
    Write-Host "Kafdrop: http://localhost:9000"
}

if ($runSpark) {
    Write-Host "Spark test: SparkPi"
    docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.2.jar 10
    Write-Host "Spark UI: http://localhost:8080"
}

if ($runSparkKafka) {
    Write-Host "Spark + Kafka test: start streaming job"
    docker exec spark-master /bin/bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 /opt/spark/tests/streaming/kafka_stream_test.py > /opt/spark/tests/streaming/kafka_stream_test.log 2>&1 &"

    Start-Sleep -Seconds 5

    $message = "test-" + (Get-Date -Format "yyyyMMdd-HHmmss")
    $message | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic test-topic

    Start-Sleep -Seconds 5

    Write-Host "Stream log (tail):"
    docker exec spark-master /bin/bash -lc "tail -n 50 /opt/spark/tests/streaming/kafka_stream_test.log"
}
