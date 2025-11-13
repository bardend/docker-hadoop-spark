import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pregunta6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pregunta6-MapReduce1")
      .getOrCreate()
    
    import spark.implicits._
    
    println("=== PREGUNTA 6: Análisis de densidad geográfica (3 MapReduce) ===")
    println("MapReduce 1: Agrupa por destino y calcula estadísticas básicas")
    println("MapReduce 2: Calcula área aproximada y densidad de lugares")
    println("MapReduce 3: Categoriza por densidad (Muy Baja a Muy Alta)")
    
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/lugares_turisticos_modified.csv")
    
    // MapReduce 1: Agrupar por destino
    val paso1 = df.filter($"LATITUD".isNotNull && $"LONGITUD".isNotNull)
      .groupBy($"DESTINO_TURISTICO")
      .agg(
        count("*").as("total_lugares"),
        avg($"LATITUD").as("centro_latitud"),
        avg($"LONGITUD").as("centro_longitud"),
        stddev($"LATITUD").as("dispersion_latitud"),
        stddev($"LONGITUD").as("dispersion_longitud")
      )
    
    // MapReduce 2: Calcular área y densidad
    val paso2 = paso1.filter($"total_lugares" > 1)
      .withColumn("area_aproximada_km2", 
        ($"dispersion_latitud" * 111.32) * 
        ($"dispersion_longitud" * 111.32)
      )
      .withColumn("densidad_lugares", 
        when($"area_aproximada_km2" > 0, $"total_lugares" / $"area_aproximada_km2")
        .otherwise(0)
      )
    
    // MapReduce 3: Categorizar
    val resultado = paso2
      .withColumn("categoria_densidad",
        when($"densidad_lugares" < 0.1, "Muy Baja")
        .when($"densidad_lugares" < 0.5, "Baja")
        .when($"densidad_lugares" < 1.0, "Media")
        .when($"densidad_lugares" < 2.0, "Alta")
        .otherwise("Muy Alta")
      )
      .select(
        $"DESTINO_TURISTICO",
        $"total_lugares",
        round($"centro_latitud", 4).as("latitud_centro"),
        round($"centro_longitud", 4).as("longitud_centro"),
        round($"area_aproximada_km2", 2).as("area_km2"),
        round($"densidad_lugares", 2).as("densidad"),
        $"categoria_densidad"
      )
      .orderBy(desc("densidad_lugares"))
    
    resultado.show(15, false)
    
    spark.stop()
  }
}
