package com.ibm.aardpfark.spark.ml.linear

import org.apache.spark.ml.linalg.{Matrix, Vector}

object SparkLinearModel {

  def apply(intercept: Double, coefficients: Vector): LinearModelData = {
    DenseLinearModelData(intercept, coefficients.toArray)
    /*
    coefficients match {
      case v: Vector =>
        DenseLinearModelData(intercept, coefficients.toArray)
        //val sv = v.toSparse
        //val map = sv.indices.map(i => (i.toString, sv(i))).toMap
        //SparseLinearModelData(intercept, map)
      // TODO auto handle dense or sparse vectors in pipeline
      //case dv: DenseVector =>
      //  DenseLinearModelData(intercept, coefficients.toArray)
      //case sv: SparseVector =>
      //  val map = sv.indices.map(i => (i.toString, sv(i))).toMap
      //  SparseLinearModelData(intercept, map)
    }
    */
  }

  def apply(intercept: Vector, coefficients: Matrix): LinearModelData = {
    MultiDenseLinearModelData(intercept.toArray, matrixToArrays(coefficients))
  }

  def matrixToArrays(m: Matrix) = {
    val (rows, cols) = (m.numRows, m.numCols)
    Array.tabulate(rows, cols) { case (i, j) => m(i, j) }.toSeq.map(_.toSeq)
  }

}
