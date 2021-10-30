#!/bin/sh

daemon_type=$1
export SPARK_USER=me
unset SPARK_MASTER_PORT  # avoid conflict with K8s service env variable
if [ "$daemon_type" == "master" ]; then
  echo "Launching a Spark Master binding to `hostname` starting on port 7077 with UI on port 8080"
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host `hostname` --port 7077 --webui-port 8080
else
  echo "Launching a Spark Worker connecting to spark://spark-master:7077 using 1 cores with 800m memory with UI on port 8081"
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker -c 1 -m 1024m --webui-port 8081 spark://spark-master:7077
fi