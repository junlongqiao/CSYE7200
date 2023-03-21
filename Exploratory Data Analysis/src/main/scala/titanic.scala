import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession


object titanic {
  // Load data
  val spark = SparkSession.builder().appName("SecondaryClassificationModel").getOrCreate()
  val data = spark.read.option("header", "true").option("inferSchema", "true").csv("data.csv")

  // Prepare data
  val assembler = new VectorAssembler().setInputCols(Array("feature1", "feature2", "feature3")).setOutputCol("features")
  val preparedData = assembler.transform(data).select("features", "label")

  // Split data into training and testing sets
  val Array(trainingData, testData) = preparedData.randomSplit(Array(0.7, 0.3), seed = 12345)

  // Define model
  val dt = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")
  val rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")
  val gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features")

  // Train model
  val dtModel = dt.fit(trainingData)
  val rfModel = rf.fit(trainingData)
  val gbtModel = gbt.fit(trainingData)

  // Evaluate model
  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
  val dtAccuracy = evaluator.evaluate(dtModel.transform(testData))
  val rfAccuracy = evaluator.evaluate(rfModel.transform(testData))
  val gbtAccuracy = evaluator.evaluate(gbtModel.transform(testData))

  println(s"Decision Tree Accuracy: $dtAccuracy")
  println(s"Random Forest Accuracy: $rfAccuracy")
  println(s"Gradient Boosted Tree Accuracy: $gbtAccuracy")

}
