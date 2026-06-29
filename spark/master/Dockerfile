FROM spark:3.5.2-scala2.12-java17-python3-ubuntu

USER root

ENV ICEBERG_VERSION=1.5.2
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_BUNDLE_VERSION=1.12.262
ENV SPARK_KAFKA_VERSION=3.5.2
ENV KAFKA_CLIENTS_VERSION=3.5.2
ENV COMMONS_POOL2_VERSION=2.11.1

RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl \
	&& rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars \
	&& curl -fL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
		https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
	&& curl -fL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
		https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
	&& curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar \
		https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar \
	&& curl -fL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
		https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
	&& curl -fL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
		https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
	&& curl -fL -o /opt/spark/jars/spark-avro_2.12-${SPARK_KAFKA_VERSION}.jar \
		https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_KAFKA_VERSION}/spark-avro_2.12-${SPARK_KAFKA_VERSION}.jar \
	&& curl -fL -o /opt/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
		https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
	&& curl -fL -o /opt/spark/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
		https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar

COPY spark/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark/jobs/ /opt/spark/jobs/
COPY schemas/ /opt/spark/schemas/
COPY tests/spark/ /opt/spark/tests/

RUN mkdir -p /opt/spark/insight-refresh /opt/spark/balancing

WORKDIR /opt/spark/jobs

EXPOSE 7077
EXPOSE 8080
