package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.pfa.document.Cell
import com.ibm.aardpfark.spark.ml.tree.{PFATreeEnsemble, TreeEnsemble, Trees}

import org.apache.spark.ml.classification.RandomForestClassificationModel

class PFARandomForestClassificationModel(
  override val sparkTransformer: RandomForestClassificationModel) extends PFATreeEnsemble {
  import com.ibm.aardpfark.pfa.dsl._

  override protected def cell = {
    val trees = sparkTransformer.trees.map(tree => Trees.generateTree(tree.rootNode)).toSeq
    val weights = sparkTransformer.treeWeights
    Cell(TreeEnsemble(trees, weights))
  }

  /**
   * Ignores weights, following [[RandomForestClassificationModel]]
   * @return
   */
  override protected def score = {
    // TODO - technically Spark uses majority vote over impurities per tree not predicted classes
    a.mode(rawScores)
  }

}
