#!/bin/bash

HOST_NAME=$1
echo "Launching a Spark Master binding to "$HOST_NAME""
/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host $HOST_NAME --port 7077 --webui-port 8080
