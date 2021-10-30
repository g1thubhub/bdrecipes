#!/bin/bash

daemon_type=$1
if [ "$daemon_type" == "master" ]; then
  echo "Launching a Spark Master binding to `hostname` starting on port "$MASTER_PORT" with UI on port "$UI_PORT""
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host `hostname` --port $MASTER_PORT --webui-port $UI_PORT
else
  echo "Launching a Spark Worker connecting to "$MASTER_URL" using "$CORES" cores with "$MEM" memory with UI on port "$UI_PORT""
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker -c $CORES -m $MEM --webui-port $UI_PORT $MASTER_URL
fi