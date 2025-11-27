// Importar funciones primero
import org.apache.spark.sql.functions._

// ===== CONEXIÓN Y LECTURA DE DATOS =====
val bienesServiciosDF = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/db_bienes_servicios").option("dbtable", "bienes_servicios").option("user", "admin").option("password", "admin").load()

// Mostrar esquema y primeras filas
bienesServiciosDF.printSchema()
bienesServiciosDF.show(10)

// ===== 1. MOSTRAR COLUMNAS ESPECÍFICAS =====
println("=== Columnas específicas: FechaOrden, NumeroOrden, Importe ===")
val columnasEspecificas = bienesServiciosDF.select("FechaOrden", "NumeroOrden", "Importe")
columnasEspecificas.show(15)

// ===== 2. UTILIZAR FILTER =====
println("=== Órdenes con Importe mayor a 10000 ===")
val filtroImporte = bienesServiciosDF.filter("Importe > 10000")
filtroImporte.show()

// ===== 3. ORDENAR INFORMACIÓN =====
println("=== Ordenar por Importe descendente ===")
val ordenadosPorImporte = bienesServiciosDF.orderBy($"Importe".desc)
ordenadosPorImporte.show(15)

// ===== 4. GROUPBY Y COUNT =====
println("=== Contar órdenes por TipoBien ===")
val conteoPorTipo = bienesServiciosDF.groupBy("TipoBien").count()
conteoPorTipo.show()

// ===== 5. PROMEDIO DE UNA COLUMNA =====
println("=== Promedio de Importe por TipoBien ===")
val promedioImporte = bienesServiciosDF.groupBy("TipoBien").agg(avg("Importe").alias("promedio_importe"))
promedioImporte.show()

// ===== 6. ESTADÍSTICAS CON FUNCIONES =====
println("=== Estadísticas de Importe por FuenteFinanciamiento ===")
val estadisticas = bienesServiciosDF.groupBy("FuenteFinanciamiento").agg(max("Importe").alias("importe_max"), min("Importe").alias("importe_min"), avg("Importe").alias("importe_promedio"), sum("Importe").alias("importe_total"), count("*").alias("total_registros"))
estadisticas.show()

// ===== 7. CONSULTAS SQL =====
bienesServiciosDF.createOrReplaceTempView("bienes_servicios")

println("=== Consulta SQL: Top 10 importes más altos ===")
val topImportes = spark.sql("SELECT FechaOrden, NumeroOrden, Importe, TipoBien FROM bienes_servicios WHERE Importe IS NOT NULL ORDER BY Importe DESC LIMIT 10")
topImportes.show()