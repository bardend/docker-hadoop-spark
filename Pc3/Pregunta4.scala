import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pregunta4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pregunta4-MayoresMenores")
      .getOrCreate()
    
    import spark.implicits._
    
    println("=== PREGUNTA 4: Estadísticas de coordenadas por destino ===")
    println("Agrupa por DESTINO_TURISTICO y calcula:")
    println("- Máximas y mínimas latitudes/longitudes")
    println("- Rangos de variación geográfica")
    println("- Total de registros por destino")
    
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/lugares_turisticos_modified.csv")
    
    val resultado = df.filter($"LATITUD".isNotNull && $"DESTINO_TURISTICO".isNotNull)
      .groupBy($"DESTINO_TURISTICO")
      .agg(
        count("*").as("total_registros"),
        max($"LATITUD").as("latitud_maxima"),
        min($"LATITUD").as("latitud_minima"),
        max($"LONGITUD").as("longitud_maxima"),
        min($"LONGITUD").as("longitud_minima"),
        (max($"LATITUD") - min($"LATITUD")).as("rango_latitud"),
        (max($"LONGITUD") - min($"LONGITUD")).as("rango_longitud")
      )
      .orderBy(desc("total_registros"))
    
    resultado.show(15, false)
    
    spark.stop()
  }
}
