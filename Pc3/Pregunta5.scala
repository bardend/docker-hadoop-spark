import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pregunta5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pregunta5-EstadisticasCompletas")
      .getOrCreate()
    
    import spark.implicits._
    
    println("=== PREGUNTA 5: Estadísticas completas de LATITUD ===")
    println("Calcula a nivel global:")
    println("- Media, promedio, desviación estándar, varianza")
    println("- Máximo, mínimo, conteo total")
    println("Luego por destino turístico")
    
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/lugares_turisticos_modified.csv")
    
    val stats = df.filter($"LATITUD".isNotNull)
      .select(
        mean($"LATITUD").as("media"),
        avg($"LATITUD").as("promedio"),
        stddev($"LATITUD").as("desviacion_estandar"),
        variance($"LATITUD").as("varianza"),
        max($"LATITUD").as("maximo"),
        min($"LATITUD").as("minimo"),
        count($"LATITUD").as("total_valores")
      )
    
    println("=== ESTADÍSTICAS GLOBALES DE LATITUD ===")
    stats.show()
    
    println("=== ESTADÍSTICAS POR DESTINO TURÍSTICO ===")
    val statsPorDestino = df.filter($"LATITUD".isNotNull && $"DESTINO_TURISTICO".isNotNull)
      .groupBy($"DESTINO_TURISTICO")
      .agg(
        count("*").as("conteo"),
        avg($"LATITUD").as("promedio_latitud"),
        stddev($"LATITUD").as("desviacion_latitud"),
        max($"LATITUD").as("max_latitud"),
        min($"LATITUD").as("min_latitud")
      )
      .orderBy(desc("conteo"))
    
    statsPorDestino.show(10, false)
    
    spark.stop()
  }
}
