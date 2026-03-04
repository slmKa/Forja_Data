#!/bin/bash
set -e
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

if [ "$SPARK_MODE" = "master" ]; then
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master --host 0.0.0.0 --port 7077 --webui-port 8080
elif [ "$SPARK_MODE" = "worker" ]; then
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://spark-master:7077
else
    exec "$@"
fi
