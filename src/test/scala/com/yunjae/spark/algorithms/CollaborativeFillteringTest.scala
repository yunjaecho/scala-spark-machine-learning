package com.yunjae.spark.algorithms

import java.time.ZonedDateTime

import com.yunjae.spark.ml.TitanicSurvival.dataFrameReader
import com.yunjae.spark.modeldeploy.ModelDeployer
import com.yunjae.spark.utils.SparkContextInitializer
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.scalatest.FunSuite

class CollaborativeFillteringTest extends FunSuite {
  private val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("should create pipline from two stages") {
   val spark = SparkContextInitializer.createSparkContext("test-CF", List.empty)
    val data = SQLContext.getOrCreate(spark)
      .createDataFrame(List(
        Rating(userId = 1, movieId = 1, rating = 5.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 1, movieId = 5, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 1, rating = 4.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 2, rating = 4.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 3, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 5, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli)
      ))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setItemCol("rating")
    val model = als.fit(data)

    val propabliityThatUserWillLinkeMovieThatWasLikeBySimilarUser =
      model.transform(SQLContext.getOrCreate(spark).createDataFrame(
        List(Rating(userID = 1, movieId = 2, rating = 5.0F))
      ))
  }



  def fileRatingSupplier(spark: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(spark)
    import sqlContext.implicits._

      spark.textFile(this.getClass.getResource("/in/sample_movielens_ratings.txt").getPath)
        .map(p => p)
        .toDF()
    }

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def createModel(spark: SparkContext, ratingsSupplier: (SparkContext) => DataFrame) = {
    val ratings = ratingsSupplier.apply(spark)
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setItemCol("rating")
    val model = als.fit(training)

    val predications = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predications)
    println(s"Root-mean-sqaure error = $rmse")

    (predications, model)
  }

}

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
