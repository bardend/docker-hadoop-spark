import org.apache.spark.sql.functions._

// Leer datos
val bienesServiciosDF = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/db_bienes_servicios").option("dbtable", "bienes_servicios").option("user", "admin").option("password", "admin").load()

bienesServiciosDF.printSchema()
bienesServiciosDF.show(10)

// Crear vista temporal
bienesServiciosDF.createOrReplaceTempView("bienes_servicios")

// CONSULTA 1: JOIN
println("\n=== CONSULTA 1: JOIN ===")
val consulta1 = spark.sql("""
  SELECT b1.NumeroRuc, b1.NumeroOrden, b1.Importe, b2.NumeroOrden AS Orden2, b2.Importe AS Importe2
  FROM bienes_servicios b1
  JOIN bienes_servicios b2 ON b1.NumeroRuc = b2.NumeroRuc AND b1.NumeroOrden < b2.NumeroOrden
  WHERE b1.Importe > 5000
  LIMIT 15
""")
consulta1.show()

// CONSULTA 2: GROUP BY con COUNT
println("\n=== CONSULTA 2: GROUP BY COUNT ===")
val consulta2 = spark.sql("""
  SELECT TipoBien, COUNT(*) AS Total
  FROM bienes_servicios
  WHERE TipoBien IS NOT NULL
  GROUP BY TipoBien
  ORDER BY Total DESC
""")
consulta2.show()

// CONSULTA 3: ORDER BY combinado
println("\n=== CONSULTA 3: ORDER BY ===")
val consulta3 = spark.sql("""
  SELECT NumeroRuc, NumeroOrden, Importe, TipoBien
  FROM bienes_servicios
  WHERE Importe > 10000
  ORDER BY Importe DESC, NumeroOrden ASC
  LIMIT 20
""")
consulta3.show()