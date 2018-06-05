package com.ibm.aardpfark.spark.ml.regression

import com.ibm.aardpfark.pfa.document.Cell
import com.ibm.aardpfark.spark.ml.tree.{PFATreeEnsemble, TreeEnsemble, Trees}

import org.apache.spark.ml.regression.GBTRegressionModel


class PFAGBTRegressionModel(override val sparkTransformer: GBTRegressionModel)
  extends PFATreeEnsemble {

  override protected def cell = {
    val trees = sparkTransformer.trees.map(tree => Trees.generateTree(tree.rootNode)).toSeq
    val weights = sparkTransformer.treeWeights
    Cell(TreeEnsemble(trees, weights))
  }

  /**
   * Weighted average prediction score
   * @return
   */
  override protected def score = {
    weightedScores
  }

}
