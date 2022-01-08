FROM gcr.io/datamechanics/spark:3.1.1-hadoop-3.2.0-java-8-scala-2.12-python-3.8-dm13

COPY tutorials/ tutorials/

COPY resources/ resources/
# Base image uses Java 8 so compile project with Java 8:
COPY target/bdrecipes-phil.jar .

COPY scripts/launch_spark_daemon.sh .

COPY scripts/launch_spark_k8daemon.sh .

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY setup.py setup.py

RUN pip3 install -e .

COPY LICENSE .

COPY LICENSE-CC3 .

COPY LICENSE-CRAWL .
# Prevents a 'cannot find name for user ID 185' in K8s pods
RUN echo 'sparkuser:x:185:0:unknown:/opt/spark/workdir:/bin/false' >> /etc/passwd