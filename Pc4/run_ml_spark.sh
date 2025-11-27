#!/bin/bash

SPARK_LOCAL_IP=127.0.0.1 spark-shell \
  --jars /home/usuario/.ivy2.5.2/jars/org.postgresql_postgresql-42.5.0.jar \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.ui.enabled=false \
  --conf "spark.driver.extraClassPath=/home/usuario/.ivy2.5.2/jars/org.postgresql_postgresql-42.5.0.jar" \
  -i ml_spark.scala