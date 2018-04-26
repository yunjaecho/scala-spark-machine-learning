package com.yunjae.spark.vectors

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class MLLibDataTypesTest extends FunSuite {
  private val spark: SparkContext = SparkSession.builder().master("local[2]").getOrCreate().sparkContext

  test("should create dense vector") {
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)

    dv.toArray.toList should contain theSameElementsAs List(
      1.0,
      0.0,
      3.0
    )
  }

  test("should create spare vector") {
    val vector: Vector = Vectors.sparse(3, Seq(
      (0, 1.0),
      (2, 3.0)
    ))

    vector.toArray.toList should contain theSameElementsAs List(
      1.0,
      0.0,
      3.0
    )
  }
}
