package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.document.Cell
import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.spark.ml.tree.{PFATreeEnsemble, TreeEnsemble, Trees}

import org.apache.spark.ml.classification.GBTClassificationModel

class PFAGBTClassificationModel(override val sparkTransformer: GBTClassificationModel)
  extends PFATreeEnsemble {

  override protected def cell = {
    val trees = sparkTransformer.trees.map(tree => Trees.generateTree(tree.rootNode)).toSeq
    val weights = sparkTransformer.treeWeights
    Cell(TreeEnsemble(trees, weights))
  }

  /**
   * Threshold the weighted average prediction score
   * @return
   */
  override protected def score = {
    If { core.gt(weightedScores, 0.0) } Then 1.0 Else 0.0
  }

}
