#!/bin/bash

echo "=========================================="
echo "  CONFIGURACIÓN INICIAL - PC3"
echo "  Setup para hadoopuser"
echo "=========================================="

#Desactivamos
${SPARK_HOME}/sbin/stop-all.sh
${HADOOP_HOME}/sbin/stop-dfs.sh

#Activamos
${SPARK_HOME}/sbin/start-all.sh
${HADOOP_HOME}/sbin/start-dfs.sh

#lugares_turisticos_modified.csv

# Subir el archivo a HDFS
# Como le digo si existe subelo :
LOCAL_FILE="${HADOOP_HOME}/lugares_turisticos_modified.csv"
HDFS_FILE="/lugares_turisticos_modified.csv"

if [ -f "$LOCAL_FILE" ]; then
  hdfs dfsadmin -safemode leave
  hdfs dfs -put -f "$LOCAL_FILE" "$HDFS_FILE"
else
  echo "❌ Archivo no encontrado: $LOCAL_FILE"
fi

# Crear carpeta de salida en Spark
mkdir -p "${SPARK_HOME}/Output"



