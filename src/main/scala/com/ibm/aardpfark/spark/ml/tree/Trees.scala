package com.ibm.aardpfark.spark.ml.tree

import java.io.ByteArrayOutputStream

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.dsl._
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.pfa.types.WithSchema
import com.ibm.aardpfark.spark.ml.PFAPredictionModel
import com.sksamuel.avro4s.{AvroNamespace, AvroOutputStream, AvroSchema, SchemaFor}
import org.apache.avro.{Schema, SchemaBuilder}
import org.json4s.native.JsonMethods.parse

import org.apache.spark.ml.tree._


@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.tree")
case class TreeEnsemble(trees: Seq[TreeNode], weights: Seq[Double]) extends WithSchema {
  def schema = AvroSchema[TreeEnsemble]
}

@AvroNamespace("com.ibm.aardpfark.exec.spark.ml.tree")
case class TreeNode(
  splitType: Int,
  featureIndex: Int,
  catVals: Array[Double],
  threshold: Double,
  pass: Either[TreeNode, Double],
  fail: Either[TreeNode, Double]) extends WithSchema {
  // implicit val schemaFor = SchemaFor[TreeNode]
  override def schema: Schema = AvroSchema[TreeNode]
}

object TreeNode {
  implicit val schemaFor = SchemaFor[TreeNode]
  def schema: Schema = AvroSchema[TreeNode]
}

object Trees {

  def json(tree: TreeNode) = {
    implicit val sf = TreeNode.schemaFor
    val baos = new ByteArrayOutputStream()
    val output = AvroOutputStream.json[TreeNode](baos)
    output.write(tree)
    output.close()
    parse(baos.toString("UTF-8"))
  }

  def generateTree(node: Node): TreeNode = {
    val tree = _generateTree(node)
    if (tree.isRight) {
      val p = tree.right.get
      TreeNode(1, 0, Array.emptyDoubleArray, 0.0, Right(p), Right(p))
    } else {
      tree.left.get
    }
  }

  private def _generateTree(node: Node): Either[TreeNode, Double] = {
    node match {
      case i: InternalNode =>
        val (splitType, catVals, threshold) = i.split match {
          case cont: ContinuousSplit =>
            (1, Array.empty[Double], cont.threshold)
          case cat: CategoricalSplit =>
            (0, cat.leftCategories, 0.0)
        }
        val leftNode = _generateTree(i.leftChild)
        val rightNode = _generateTree(i.rightChild)
        val node = TreeNode(splitType, i.split.featureIndex, catVals, threshold, leftNode, rightNode)
        Left(node)
      case l: LeafNode =>
        Right(l.prediction)
    }

  }
}

trait PFADecisionTreeModel extends PFAPredictionModel[TreeNode] {

  protected def tree: TreeNode

  override def cell = Cell(tree)
  private val inputParam = Param("f", inputSchema)
  private val treeParam = Param("t", tree.schema, simpleSchema = true)

  private val fIdx = Let("fIdx", Attr(treeParam.ref, "featureIndex"))
  private val fVal = Let("fVal", Attr(inputParam.ref, "features", fIdx.ref))
  private val threshold = Let("threshold", Attr(treeParam.ref, "threshold"))
  private val splitType = Let("splitType", Attr(treeParam.ref, "splitType"))
  private val catVals = Let("catVals", Attr(treeParam.ref, "catVals"))
  private val test = If {
    core.eq(splitType.ref, 1)
  } Then {
    core.lte(fVal.ref, threshold.ref)
  } Else {
    a.contains(catVals.ref, fVal.ref)
  }

  private val featuresRecord = NewRecord(inputSchema, Map(featuresCol -> inputExpr))

  private val treeTest = NamedFunctionDef("treeTest", FunctionDef[Boolean](
    Seq(inputParam, treeParam),
    Seq(fIdx, fVal, threshold, splitType, catVals, test): Seq[PFAExpression]
  ))

  override def action: PFAExpression = {
    val treeWalk = model.tree.simpleWalk(featuresRecord, modelCell.ref, treeTest.ref)
    NewRecord(outputSchema, Map(predictionCol -> treeWalk))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withFunction(treeTest)
      .withAction(action)
      .pfa
  }
}


trait PFATreeEnsemble extends PFAPredictionModel[TreeEnsemble] {

  //override protected val inputExpr = StringExpr("input")

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(featuresCol).`type`().array().items().doubleType().noDefault()
      .endRecord()
  }

  private val inputParam = Param("f", inputSchema)
  private val treeParam = Param("t", TreeNode.schema, simpleSchema = true)

  private val fIdx = Let("fIdx", Attr(treeParam.ref, "featureIndex"))
  private val fVal = Let("fVal", Attr(inputParam.ref, "features", fIdx.ref))
  private val threshold = Let("threshold", Attr(treeParam.ref, "threshold"))
  private val splitType = Let("splitType", Attr(treeParam.ref, "splitType"))
  private val catVals = Let("catVals", Attr(treeParam.ref, "catVals"))
  private val test = If {
    core.eq(splitType.ref, 1)
  } Then {
    core.lte(fVal.ref, threshold.ref)
  } Else {
    a.contains(catVals.ref, fVal.ref)
  }

  private val featuresRecord = NewRecord(inputSchema, Map(featuresCol -> inputExpr))

  private val doubleMult = NamedFunctionDef("doubleMult", FunctionDef[Double, Double](
    Seq("a", "b"), Seq(core.mult("a", "b"))))

  private val treeTestFn = NamedFunctionDef("treeTest", FunctionDef[Boolean](
    Seq(inputParam, treeParam),
    Seq(fIdx, fVal, threshold, splitType, catVals, test): Seq[PFAExpression]
  ))

  private val treeWalkFn = NamedFunctionDef("treeWalk", FunctionDef[Double](
    Seq(inputParam, treeParam),
    Seq(model.tree.simpleWalk(inputParam.ref, treeParam.ref, treeTestFn.ref))
  ))

  private val fill = treeWalkFn.fill(featuresRecord)

  protected def score: PFAExpression

  protected def rawScores: PFAExpression = a.map(modelCell.ref("trees"), fill)

  protected def weightedScores: PFAExpression = {
    val weights = modelCell.ref("weights")
    a.sum(
      a.zipmap(rawScores, weights, doubleMult.ref)
    )
  }

  override def action: PFAExpression = {
    NewRecord(outputSchema, Map(predictionCol -> score))
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withFunction(treeTestFn)
      .withFunction(doubleMult)
      .withFunction(treeWalkFn)
      .withAction(action)
      .pfa
  }
}