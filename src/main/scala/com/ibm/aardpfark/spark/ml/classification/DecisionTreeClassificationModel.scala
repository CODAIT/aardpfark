package com.ibm.aardpfark.spark.ml.classification

import com.ibm.aardpfark.spark.ml.tree.{PFADecisionTreeModel, TreeNode, Trees}

import org.apache.spark.ml.classification.DecisionTreeClassificationModel

class PFADecisionTreeClassificationModel(override val sparkTransformer: DecisionTreeClassificationModel)
  extends PFADecisionTreeModel {

  override protected def tree: TreeNode = Trees.generateTree(sparkTransformer.rootNode)

}
