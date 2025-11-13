import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pregunta2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pregunta2-FiltroFechas")
      .getOrCreate()
    
    import spark.implicits._
    
    println("=== PREGUNTA 2: Filtrado por fechas con muestra aleatoria ===")
    println("Filtra registros entre 2023-01-01 y 2024-12-31")
    println("Toma muestra aleatoria del 40%")
    println("Muestra nombre, destino, ciudad y fecha")
    
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/lugares_turisticos_modified.csv")
    
    val dfWithDate = df.withColumn("FECHA", to_date($"FECHA_CORTE", "yyyy-MM-dd"))
    
    val totalFiltrado = dfWithDate.filter($"FECHA".between("2023-01-01", "2024-12-31")).count()
    
    val resultado = dfWithDate
      .filter($"FECHA".between("2023-01-01", "2024-12-31"))
      .sample(0.4)
      .select($"NOMBRE_LUGAR_TURISTICO", $"DESTINO_TURISTICO", $"CIUDAD", $"FECHA")
      .orderBy(desc("FECHA"))
    
    resultado.show(15, false)
    println(s"Registros en el rango de fechas: $totalFiltrado")
    println(s"Muestra aleatoria (40%): ${resultado.count()} registros")
    
    spark.stop()
  }
}
