package com.ibm.aardpfark.spark.ml.regression

import com.ibm.aardpfark.pfa.document.Cell
import com.ibm.aardpfark.spark.ml.tree.{PFATreeEnsemble, TreeEnsemble, Trees}

import org.apache.spark.ml.regression.RandomForestRegressionModel

class PFARandomForestRegressionModel(override val sparkTransformer: RandomForestRegressionModel)
  extends PFATreeEnsemble {
  import com.ibm.aardpfark.pfa.dsl._

  override protected def cell = {
    val trees = sparkTransformer.trees.map(tree => Trees.generateTree(tree.rootNode)).toSeq
    val weights = sparkTransformer.treeWeights
    Cell(TreeEnsemble(trees, weights))
  }

  /**
   * Ignores weights following [[RandomForestRegressionModel]]
   * @return
   */
  def score = {
    a.mean(rawScores)
  }

}
