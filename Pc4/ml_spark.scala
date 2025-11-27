import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._

// ========== CARGAR DATOS ==========
println("\n=== CARGANDO DATOS ===")
val rawData = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/db_bienes_servicios").option("dbtable", "bienes_servicios").option("user", "admin").option("password", "admin").load()

println(s"Total registros: ${rawData.count()}")
rawData.show(5)

// ========== PREPARAR DATOS PARA CLASIFICACIÓN ==========
// Predecir TipoBien basado en Importe, NumeroOrden, etc
println("\n=== PREPARANDO DATOS PARA CLASIFICACIÓN ===")
val dataClasif = rawData.select("Importe", "NumeroOrden", "Cantidad", "TipoBien").filter("TipoBien IS NOT NULL AND Importe IS NOT NULL AND Cantidad IS NOT NULL").na.drop()

println(s"Registros limpios clasificación: ${dataClasif.count()}")
dataClasif.show(5)

// Indexar label
val labelIndexer = new StringIndexer().setInputCol("TipoBien").setOutputCol("label").fit(dataClasif)

// Crear vector de features
val assembler = new VectorAssembler().setInputCols(Array("Importe", "NumeroOrden", "Cantidad")).setOutputCol("features")

// Dividir datos
val Array(trainClasif, testClasif) = dataClasif.randomSplit(Array(0.7, 0.3), seed = 123)
println(s"Train: ${trainClasif.count()}, Test: ${testClasif.count()}")

// ========== MODELO DE CLASIFICACIÓN: DECISION TREE ==========
println("\n=== ENTRENANDO DECISION TREE ===")
val dt = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")
val pipelineClasif = new Pipeline().setStages(Array(labelIndexer, assembler, dt))
val modelClasif = pipelineClasif.fit(trainClasif)
val predictionsClasif = modelClasif.transform(testClasif)

// Mostrar predicciones
println("\n=== PREDICCIONES CLASIFICACIÓN ===")
predictionsClasif.select("TipoBien", "prediction", "Importe", "Cantidad").show(10)

// Evaluar
val evaluatorAcc = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
val evaluatorF1 = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("f1")
val evaluatorRecall = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("weightedRecall")

val accuracy = evaluatorAcc.evaluate(predictionsClasif)
val f1 = evaluatorF1.evaluate(predictionsClasif)
val recall = evaluatorRecall.evaluate(predictionsClasif)

println("\n=== MÉTRICAS CLASIFICACIÓN ===")
println(f"Accuracy: $accuracy%.4f")
println(f"F1-Score: $f1%.4f")
println(f"Recall: $recall%.4f")

// ========== PREPARAR DATOS PARA REGRESIÓN ==========
// Predecir Importe basado en Cantidad, NumeroOrden
println("\n\n=== PREPARANDO DATOS PARA REGRESIÓN ===")
val dataReg = rawData.select("Importe", "Cantidad", "NumeroOrden").filter("Importe IS NOT NULL AND Cantidad IS NOT NULL AND Importe > 0").na.drop()

println(s"Registros limpios regresión: ${dataReg.count()}")
dataReg.show(5)

// Renombrar para regresión
val dataRegRenamed = dataReg.withColumnRenamed("Importe", "label")

// Crear vector features
val assemblerReg = new VectorAssembler().setInputCols(Array("Cantidad", "NumeroOrden")).setOutputCol("features")

// Dividir datos
val Array(trainReg, testReg) = dataRegRenamed.randomSplit(Array(0.7, 0.3), seed = 123)
println(s"Train: ${trainReg.count()}, Test: ${testReg.count()}")

// ========== MODELO DE REGRESIÓN: LINEAR REGRESSION ==========
println("\n=== ENTRENANDO LINEAR REGRESSION ===")
val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(10)
val pipelineReg = new Pipeline().setStages(Array(assemblerReg, lr))
val modelReg = pipelineReg.fit(trainReg)
val predictionsReg = modelReg.transform(testReg)

// Mostrar predicciones
println("\n=== PREDICCIONES REGRESIÓN ===")
predictionsReg.select("label", "prediction", "Cantidad", "NumeroOrden").show(10)

// Evaluar
val evaluatorRMSE = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
val evaluatorR2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2")
val evaluatorMAE = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("mae")

val rmse = evaluatorRMSE.evaluate(predictionsReg)
val r2 = evaluatorR2.evaluate(predictionsReg)
val mae = evaluatorMAE.evaluate(predictionsReg)

println("\n=== MÉTRICAS REGRESIÓN ===")
println(f"RMSE: $rmse%.4f")
println(f"R2: $r2%.4f")
println(f"MAE: $mae%.4f")

println("\n=== RESUMEN FINAL ===")
println("CLASIFICACIÓN (Decision Tree):")
println(f"  Accuracy: $accuracy%.4f")
println(f"  F1-Score: $f1%.4f")
println(f"  Recall: $recall%.4f")
println("\nREGRESIÓN (Linear Regression):")
println(f"  RMSE: $rmse%.4f")
println(f"  R2: $r2%.4f")
println(f"  MAE: $mae%.4f")