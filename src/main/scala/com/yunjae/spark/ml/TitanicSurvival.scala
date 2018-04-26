package com.yunjae.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Bucketizer, Normalizer, StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{SQLContext, SparkSession}

object TitanicSurvival extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  // SparkSql SparkSession used
  val session = SparkSession.builder().appName("TitanicSurvival").master("local[1]").getOrCreate()

  val dataFrameReader = session.read

  var dataFrame = dataFrameReader
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("in/train.csv")
    .cache()

  val schema = dataFrame.schema

  dataFrame.printSchema()

  dataFrame.show()
  //dataFrame.describe().show()

  val toDouble = session.udf.register("toDouble", ((n: Int) => n.toDouble))
  val avgAge = dataFrame.select("Age").first()(0).asInstanceOf[Double]

  val fillDataFrame = dataFrame.na.fill(avgAge, Seq("Age"))
    .drop("Name").drop("Cabin").drop("Embarked")
    .withColumn("Survived", toDouble(dataFrame("Survived")))


  //=========================
  // indexer
  //=========================
  val sexIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")
  val sexIndexerDataFrame = sexIndexer.fit(dataFrame).transform(dataFrame).select("Pclass", "Age", "SibSp", "Parch", "Sex", "Fare", "SexIndex")

  sexIndexerDataFrame.describe().show()
  sexIndexerDataFrame.printSchema()

  //=========================
  // classification (등급화)
  //=========================
  val fareSplits = Array(0.0, 50.0, 100.0, 150.0, 200.0, Double.PositiveInfinity)
  val fareBucket = new Bucketizer().setInputCol("Fare").setOutputCol("FareBucket").setSplits(fareSplits)
  val fareBucketDataFrame = fareBucket.transform(dataFrame).select("Pclass", "Age", "SibSp", "Parch","FareBucket")

  //=========================
  // VectorAssembler
  //=========================
  val assembler = new VectorAssembler()
    .setInputCols(Array("Pclass", "Age", "SibSp", "Parch","FareBucket" ))
    .setOutputCol("tmpFeatures")

  val normalizer = new Normalizer().setInputCol("tmpFeatures").setOutputCol("features")

  val logres = new LogisticRegression().setMaxIter(10)
  logres.setLabelCol("Survived")

  val pipeline = new Pipeline().setStages(Array(fareBucket, sexIndexer, assembler, normalizer, logres))

  // 7: 3 기준으로 train, test 데이터 분리 처리
  val splits = fillDataFrame.randomSplit(Array(0.7 ,0.3), seed = 9L)
  val train = splits(0).cache()
  val test = splits(1).cache()

  val model = pipeline.fit(train)
  val result = model.transform(test)

  result.show()

  val predictionAndLable = result.select("prediction", "Survived").map(row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double])).
  val metrics = new BinaryClassificationMetrics(predictionAndLable)

}
