package com.yunjae.spark.vectors

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers._

/**
  * A local vector has integer-typed and 0-based
  * indices and double-typed values, stored on a single machine
  * Mlib supports two types of local vectors: dense and sparse
  */
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

  // a sparse vector is backed by two parallel arrays: indices and values
  // it is more space effective for bigger data sets
  test("should create spare vector") {
    val vector: Vector = Vectors.sparse(4, Seq(
      (0, 1.0),
      (2, 3.0)
    ))

    vector.toArray.toList should contain theSameElementsAs List(
      1.0,
      0.0,
      3.0,
      0.0
    )
  }

  /**
    * A labeled point is a local vector,
    * either dense or sparse,
    * associated with a label/response.
    * In MLlib, labeled points are used in supervised learning algorithms.
    * We use a double to store a label, so we can use labeled points in both
    * regression and classification.
    * For binary classification, a label should be either 0 (negative) or 1 (positive).
    * For multiclass classification, labels should be class indices starting from zero:
    * 0, 1, 2, ....
    */
  test("should construct LabeledPoint") {
    // Create a labeled point with a positive label and a dense feature vector.
    val positiveLabeledPoint = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

    // Create a labeled point with a negative label and a sparse feature vector.
    val negativeLabeledPoint = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2),
      Array(1.0, 3.0)))

    assert(positiveLabeledPoint.label == 1.0)
    assert(negativeLabeledPoint.label == 0)
  }

  test("should create local Matrix") {
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    dm.colIter.toList should contain theSameElementsAs List(
      Vectors.dense(1.0, 3.0, 5.0),
      Vectors.dense(2.0, 4.0, 6.0)
    )
  }

  /**
    * A distributed matrix has long-typed row and column indices and
    * double-typed values, stored distributively in one or more RDDs.
    */
  test("should create distributed matrix using RDD") {
    //given
    val rows: RDD[Vector] = spark.makeRDD(
      List(
        Vectors.dense(1.0, 2.0, 3.0),
        Vectors.dense(11.0, 12.0, 13.0)
      )
    )
    //when
    val mat: RowMatrix = new RowMatrix(rows)

    val numberOfRows = mat.numRows()
    val numberOfColumns = mat.numCols()

    //then
    assert(numberOfRows == 2)
    assert(numberOfColumns == 3)
  }

}
