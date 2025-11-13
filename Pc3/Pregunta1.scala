import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pregunta1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pregunta1-TresCampos")
      .getOrCreate()
    
    import spark.implicits._
    
    println("=== PREGUNTA 1: Lugares turísticos por destino con coordenadas ===")
    println("Agrupa por DESTINO_TURISTICO y calcula:")
    println("- Total de lugares por destino")
    println("- Número de ciudades únicas") 
    println("- Promedio de latitud y longitud")
    
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/lugares_turisticos_modified.csv")
    
    val resultado = df.select($"DESTINO_TURISTICO", $"NOMBRE_LUGAR_TURISTICO", $"LATITUD", $"LONGITUD", $"CIUDAD")
      .filter($"LATITUD".isNotNull && $"LONGITUD".isNotNull && $"DESTINO_TURISTICO".isNotNull)
      .groupBy($"DESTINO_TURISTICO")
      .agg(
        count($"NOMBRE_LUGAR_TURISTICO").as("total_lugares"),
        countDistinct($"CIUDAD").as("ciudades_unicas"),
        avg($"LATITUD").as("latitud_promedio"),
        avg($"LONGITUD").as("longitud_promedio")
      )
      .orderBy(desc("total_lugares"))
    
    resultado.show(20, false)
    println(s"Total de destinos turísticos: ${resultado.count()}")
    
    spark.stop()
  }
}
