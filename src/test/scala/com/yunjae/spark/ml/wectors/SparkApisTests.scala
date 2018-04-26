package com.yunjae.spark.ml.wectors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class SparkApisTests extends FunSuite {
  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  test("rdd") {
    val rdd: RDD[String] =
      spark.sparkContext.parallelize(Array("a", "b", "c", "d"))

    val res = rdd.map(_.toUpperCase).collect().toList

    res should contain theSameElementsAs List(
      "A", "B", "C", "D"
    )
  }

  test("dataFrame") {
    import spark.sqlContext.implicits._

    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2")
      )).toDF()

    val userDataForUserIda = userData
      .where("userId = 'a'")
      .count()

    assert(userDataForUserIda == 1)
  }

  /*test("dataSet akka typed DataFrame") {
    import spark.sqlContext.implicits._

    val userData =
      spark.sparkContext.makeRDD(List(
        UserData("a", "1"),
        UserData("b", "2")
      )).toDF()
    userData.createOrReplaceTempView("user_data")


    val userDataForUserIda = userData
      .filter(_.userId == "a")
      .count()

    assert(userDataForUserIda == 1)
  }*/


}

case class UserData(userId: String, data: String)

