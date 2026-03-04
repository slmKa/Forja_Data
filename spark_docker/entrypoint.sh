#!/bin/bash
set -e

case "$1" in
  master)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ;;
  worker)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "$2"
    ;;
  *)
    exec "$@"
    ;;
esac
