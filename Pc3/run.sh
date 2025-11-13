#!/bin/bash

# Configuración
SPARK_HOME="$HOME/spark"
MASTER="spark://Omen:7077"

echo "=== INICIANDO EJECUCIÓN DE LAS PRIMERAS 6 PREGUNTAS ==="

# Limpiar compilaciones anteriores
echo "Limpiando compilaciones anteriores..."
rm -f *.class
rm -f *.jar

# Compilar cada pregunta
echo "Compilando archivos Scala..."
for i in {1..6}; do
    echo "Compilando Pregunta$i.scala"
    echo "---------------------------------------------------------------------------------------"
    scalac -cp "$SPARK_HOME/jars/*:." "Pregunta$i.scala"
    if [ $? -ne 0 ]; then
        echo "ERROR al compilar Pregunta$i.scala"
        exit 1
    fi
done

# Crear JAR con todas las clases
echo "Creando JAR..."
jar cf tarea-spark.jar *.class

# Verificar que el JAR se creó
if [ ! -f "tarea-spark.jar" ]; then
    echo "ERROR: No se pudo crear el JAR"
    exit 1
fi

echo "JAR creado exitosamente. Tamaño: $(du -h tarea-spark.jar | cut -f1)"

# Ejecutar cada pregunta
for i in {1..6}; do
    echo ""
    echo "=== EJECUTANDO PREGUNTA $i ==="
    
    $SPARK_HOME/bin/spark-submit \
        --master $MASTER \
        --class "Pregunta$i" \
        --deploy-mode client \
        tarea-spark.jar
    
    if [ $? -eq 0 ]; then
        echo "✓ Pregunta $i completada exitosamente."
    else
        echo "✗ Error en Pregunta $i"
    fi
    sleep 2
done

# Limpiar
echo "Limpiando archivos temporales..."
rm -f *.class
rm -f tarea-spark.jar

echo ""
echo "=== TODAS LAS PREGUNTAS COMPLETADAS ==="
