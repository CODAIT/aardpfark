package com.ibm.aardpfark.spark.ml.feature

import com.ibm.aardpfark.pfa.document.{Cell, PFABuilder, PFADocument}
import com.ibm.aardpfark.pfa.expression._
import com.ibm.aardpfark.spark.ml.PFAModel
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.feature.CountVectorizerModel


class PFACountVectorizerModel(override val sparkTransformer: CountVectorizerModel)
  extends PFAModel[Vocab] {
  import com.ibm.aardpfark.pfa.dsl._

  private val inputCol = sparkTransformer.getInputCol
  private val outputCol = sparkTransformer.getOutputCol
  private val inputExpr = StringExpr(s"input.${inputCol}")

  private val minTF = sparkTransformer.getMinTF
  private val binary = sparkTransformer.getBinary

  override def inputSchema = {
    SchemaBuilder.record(withUid(inputBaseName)).fields()
      .name(inputCol).`type`().array().items().stringType().noDefault()
      .endRecord()
  }

  override def outputSchema = {
    SchemaBuilder.record(withUid(outputBaseName)).fields()
      .name(outputCol).`type`().array().items().doubleType().noDefault()
      .endRecord()
  }

  // vocab cell storage
  override protected def cell = {
    val vocab = Vocab(sparkTransformer.vocabulary.zipWithIndex.toMap)
    Cell(vocab)
  }

  private val vocabRef = modelCell.ref("vocab")

  private val mapFn = NamedFunctionDef("mapFn", FunctionDef[Double](
    Seq(Param[Int]("x"), Param[Double]("v")),
    Seq( If(core.gte(StringExpr("v"), core.mult(StringExpr("x"), minTF))) Then {
      StringExpr("v")
    } Else {
      0.0
    })
  ))

  private val binaryFn = NamedFunctionDef("toBinary", FunctionDef[Double, Double]("v",
    If(core.gte(StringExpr("v"), 1.0)) Then {
      1.0
    } Else {
      0.0
    }
  ))

  override def action: PFAExpression = {
    // create a placeholder array for output features
    val arraySchema = SchemaBuilder.array().items().doubleType()
    val features = Let("f", NewArray(arraySchema, Seq()))
    val f = features.ref
    // placeholder for token count
    val tc = Let("tc", 0)

    val word = StringExpr("word")
    val wordRef = modelCell.ref("vocab", word)
    val idxDef = Let("idx", cast.json(wordRef))
    val idx = idxDef.ref


    val initMap = ForEach(word, map.keys(vocabRef)) { w =>
      // Initialize array containing 0s for each word in vocab
      Seq(
        If(map.containsKey(vocabRef, word)) Then {
          Action(idxDef, Set(f, a.append(f, 0.0)))
        }
      )
    }

    val foreach = ForEach(word, inputExpr) { w =>
      val idxExpr = cast.int(Attr(vocabRef, word))
      val addExpr = core.plus(1.0, Attr(f, idxExpr))

      val set = Set(f, a.replace(f, idxExpr, addExpr))
      Seq(
        If(map.containsKey(vocabRef, word)) Then {
          Action(idxDef, set)
        },
        Set(tc.ref, core.plus(tc.ref, 1))
      )
    }

    val minTfFilter = if (minTF >= 1.0) {
      mapFn.fill(1)
    } else {
      mapFn.fill(tc.ref)
    }

    val filterMap = if (binary) {
      a.map(a.map(f, minTfFilter), binaryFn.ref)
    } else {
      a.map(f, minTfFilter)
    }

    Action(
      tc,
      features,
      initMap,
      foreach,
      NewRecord(outputSchema, Map(outputCol -> filterMap))
    )
  }

  override def pfa: PFADocument = {
    PFABuilder()
      .withName(sparkTransformer.uid)
      .withMetadata(getMetadata)
      .withInput(inputSchema)
      .withOutput(outputSchema)
      .withCell(modelCell)
      .withAction(action)
      .withFunction(mapFn)
      .withFunction(binaryFn)
      .pfa
  }
}
