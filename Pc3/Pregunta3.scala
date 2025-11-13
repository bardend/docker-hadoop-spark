import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pregunta3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Pregunta3-BusquedaTexto")
      .getOrCreate()
    
    import spark.implicits._
    
    println("=== PREGUNTA 3: Búsqueda de cascadas/cataratas ===")
    println("Busca en NOMBRE_LUGAR_TURISTICO textos que contengan 'catarata' o 'cascada'")
    println("Agrupa por DESTINO_TURISTICO y cuenta ocurrencias")
    
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/lugares_turisticos_modified.csv")
    
    val resultado = df.filter(
        lower($"NOMBRE_LUGAR_TURISTICO").contains("catarata") || 
        lower($"NOMBRE_LUGAR_TURISTICO").contains("cascada")
      )
      .groupBy($"DESTINO_TURISTICO")
      .agg(
        count("*").as("total_cataratas_cascadas"),
        collect_list($"NOMBRE_LUGAR_TURISTICO").as("nombres")
      )
      .orderBy(desc("total_cataratas_cascadas"))
    
    resultado.show(10, false)
    
    println("=== DETALLES ENCONTRADOS ===")
    df.filter(
        lower($"NOMBRE_LUGAR_TURISTICO").contains("catarata") || 
        lower($"NOMBRE_LUGAR_TURISTICO").contains("cascada")
      )
      .select($"NOMBRE_LUGAR_TURISTICO", $"DESTINO_TURISTICO", $"CIUDAD")
      .show(10, false)
    
    spark.stop()
  }
}
