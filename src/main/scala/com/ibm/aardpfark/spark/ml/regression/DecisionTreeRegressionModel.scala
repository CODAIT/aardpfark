package com.ibm.aardpfark.spark.ml.regression

import com.ibm.aardpfark.spark.ml.tree.{PFADecisionTreeModel, TreeNode, Trees}

import org.apache.spark.ml.regression.DecisionTreeRegressionModel

class PFADecisionTreeRegressionModel(override val sparkTransformer: DecisionTreeRegressionModel)
  extends PFADecisionTreeModel {
  override protected def tree: TreeNode = Trees.generateTree(sparkTransformer.rootNode)
}
