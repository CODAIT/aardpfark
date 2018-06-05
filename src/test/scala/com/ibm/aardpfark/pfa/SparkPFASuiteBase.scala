package com.ibm.aardpfark.pfa

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalactic.Equality
import org.scalatest.FunSuite

abstract class SparkPFASuiteBase extends FunSuite with DataFrameSuiteBase with PFATestUtils {

  val sparkTransformer: Transformer
  val input: Array[String]
  val expectedOutput: Array[String]

  val sparkConf =  new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID).
    set("spark.driver.host", "localhost")
  override lazy val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  override val reuseContextIfPossible = true

  // Converts column containing a vector to an array
  def withColumnAsArray(df: DataFrame, colName: String) = {
    val vecToArray = udf { v: Vector => v.toArray }
    df.withColumn(colName, vecToArray(df(colName)))
  }

  def withColumnAsArray(df: DataFrame, first: String, others: String*) = {
    val vecToArray = udf { v: Vector => v.toArray }
    var result = df.withColumn(first, vecToArray(df(first)))
    others.foreach(c => result = result.withColumn(c, vecToArray(df(c))))
    result
  }

  // Converts column containing a vector to a sparse vector represented as a map
  def getColumnAsSparseVectorMap(df: DataFrame, colName: String) = {
    val vecToMap = udf { v: Vector => v.toSparse.indices.map(i => (i.toString, v(i))).toMap }
    df.withColumn(colName, vecToMap(df(colName)))
  }

}

abstract class Result

object ApproxEquality extends ApproxEquality

trait ApproxEquality {

  import org.scalactic.Tolerance._
  import org.scalactic.TripleEquals._

  implicit val seqApproxEq: Equality[Seq[Double]] = new Equality[Seq[Double]] {
    override def areEqual(a: Seq[Double], b: Any): Boolean = {
      b match {
        case d: Seq[Double] =>
          a.zip(d).forall { case (l, r) => l === r +- 0.001 }
        case _ =>
          false
      }
    }
  }

  implicit val vectorApproxEq: Equality[Vector] = new Equality[Vector] {
    override def areEqual(a: Vector, b: Any): Boolean = {
      b match {
        case v: Vector =>
          a.toArray.zip(v.toArray).forall { case (l, r) => l === r +- 0.001 }
        case _ =>
          false
      }
    }
  }
}