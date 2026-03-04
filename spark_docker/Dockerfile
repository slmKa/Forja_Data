FROM python:3.11-slim-bullseye

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl openjdk-11-jdk procps bash \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://repo.huaweicloud.com/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && mkdir -p ${SPARK_HOME}/logs

RUN pip install --no-cache-dir pyspark==${SPARK_VERSION} kafka-python boto3

COPY start.sh /start.sh
COPY jars/*.jar /opt/spark/jars/
RUN chmod +x /start.sh

WORKDIR ${SPARK_HOME}
ENTRYPOINT ["/bin/bash", "/start.sh"]
