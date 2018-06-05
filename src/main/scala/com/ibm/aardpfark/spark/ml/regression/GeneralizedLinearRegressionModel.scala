package com.ibm.aardpfark.spark.ml.regression

import com.ibm.aardpfark.pfa.expression.PFAExpression
import com.ibm.aardpfark.spark.ml.PFALinearPredictionModel
import org.apache.avro.SchemaBuilder

import org.apache.spark.ml.regression.{GeneralizedLinearRegressionModel => GLRModel}

class PFAGeneralizedLinearRegressionModel(override val sparkTransformer: GLRModel)
  extends PFALinearPredictionModel {
  import com.ibm.aardpfark.pfa.dsl._

  private val epsilon = 1e-16
  private val linkPredictionCol = if (sparkTransformer.isSet(sparkTransformer.linkPredictionCol)) {
    Some(sparkTransformer.getLinkPredictionCol)
  } else {
    None
  }
  private val family = sparkTransformer.getFamily
  private val variancePower = sparkTransformer.getVariancePower
  private val linkPower = if (sparkTransformer.isSet(sparkTransformer.linkPower)) {
    sparkTransformer.getLinkPower
  } else {
    1.0 - variancePower
  }
  private val link: String = if (sparkTransformer.isSet(sparkTransformer.link)) {
    sparkTransformer.getLink
  } else {
    // defaults from Spark
    family match {
      case "gaussian" =>
        "identity"
      case "binomial" =>
        "logit"
      case "poisson" =>
        "log"
      case "gamma" =>
        "inverse"
      case "tweedie" =>
        linkPower match {
          case 0.0 =>
            "log"
          case 1.0 =>
            "identity"
          case -1.0 =>
            "inverse"
          case 0.5 =>
            "sqrt"
          case other =>
            "power"
        }
    }
  }


  override def outputSchema = {
    val fieldBldr = SchemaBuilder.record(withUid(outputBaseName)).fields()
      linkPredictionCol.foreach { lp =>
        fieldBldr.name(lp).`type`().doubleType().noDefault()
      }
      fieldBldr
        .name(predictionCol).`type`.doubleType().noDefault()
        .endRecord()
  }

  private def unlinkFn(eta: PFAExpression) = {
    link match {
      case "identity" =>
        eta
      case "logit" =>
        m.link.logit(eta)
      case "log" =>
        m.exp(eta)
      case "sqrt" =>
        core.pow(eta, 2)
      case "cloglog" =>
        m.link.cloglog(eta)
      case "probit" =>
        m.link.probit(eta)
      case "inverse" =>
        core.div(1.0, eta)
      case "power" =>
        if (linkPower == 0.0) {
          m.exp(eta)
        } else {
          core.pow(eta, 1.0 / linkPower)
        }
    }
  }

  private def projectFn(mu: PFAExpression) = {
    family match {
      case "gamma" | "poisson" | "tweedie" =>
        If { core.lt(mu, epsilon) } Then {
          epsilon
        } Else {
          mu
        }
      case _ =>
        mu
    }
  }
  override def action = {
    val outputFields: Map[String, PFAExpression] =
      Map(predictionCol -> projectFn(unlinkFn(margin.ref))) ++
        linkPredictionCol.map { lp => Map(lp -> margin.ref) }.getOrElse(Map())
    Action(
      margin,
      NewRecord(outputSchema, outputFields)
    )
  }
}
