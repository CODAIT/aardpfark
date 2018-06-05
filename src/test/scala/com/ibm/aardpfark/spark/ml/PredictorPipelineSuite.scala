package com.ibm.aardpfark.spark.ml

import scala.util.Random

import com.ibm.aardpfark.pfa.{Result, SparkPipelinePFASuiteBase}

import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder


case class KMeansPipelineResult(
  features: Seq[Double],
  ewp: Seq[Double],
  pca: Seq[Double],
  s1: Seq[Double],
  s2: Seq[Double],
  s3: Seq[Double],
  kmeans: Seq[Double],
  prediction: Double
) extends Result

class KMeansPipelineSuite extends SparkPipelinePFASuiteBase[KMeansPipelineResult] {

  val inputPath = "data/sample_lda_libsvm_data.txt"
  val data = spark.read.format("libsvm").load(inputPath)

  implicit val encoder = ExpressionEncoder[Vector]
  val size = data.select("features").as[Vector].first().size
  val random = new Random(42)
  val scalingVec = Vectors.dense(Array.fill(size) { random.nextGaussian() })

  override protected val schema = data.schema

  val pipeline = new Pipeline()

  val ewp = new ElementwiseProduct()
    .setInputCol("features")
    .setOutputCol("ewp")
    .setScalingVec(scalingVec)

  val pca = new PCA()
    .setInputCol("ewp")
    .setOutputCol("pca")
    .setK(8)

  val scaler1 = new MinMaxScaler().setInputCol("pca").setOutputCol("s1")
  val scaler2 = new MaxAbsScaler().setInputCol("s1").setOutputCol("s2")
  val scaler3 = new StandardScaler().setInputCol("s2").setOutputCol("s3")

  val slicer = new VectorSlicer()
    .setInputCol("s3")
    .setIndices(Array(0, 1, 2, 3, 4))
    .setOutputCol("kmeans")

  val kmeans = new KMeans()
    .setFeaturesCol("kmeans")
    .setK(3)

  val stages: Seq[PipelineStage] = Seq(ewp, pca, scaler1, scaler2, scaler3, slicer, kmeans)
  pipeline.setStages(stages.toArray)

  override val sparkTransformer = pipeline.fit(data)
  val sparkOutput = sparkTransformer.transform(data)
  override val input = withColumnAsArray(data, "features").toJSON.collect()
  override val expectedOutput = withColumnAsArray(sparkOutput,
    "features", "ewp", "pca", "s1", "s2", "s3", "kmeans").toJSON.collect()

}

class PredictorPipelineSuite extends SparkPipelinePFASuiteBase[PipelineResult] {

  val data = spark.createDataFrame(Seq(
    ("no", "a", "foo", 1.0, 22),
    ("yes", "b", "bar", 2.0, 43),
    ("no", "c", "bar", 1.4, 34),
    ("no", "a", "bar", 0.4, 19),
    ("yes", "a", "foo", -0.1, 56),
    ("no", "c", "bar", 2.3, 37),
    ("yes", "d", "baz", 2.1, 76)
  )).toDF("label_str", "cat1", "cat2", "real1", "real2")

  override protected val schema = data.schema

  val featureStages = {
    val labelIndexer = new StringIndexer()
      .setInputCol("label_str")
      .setOutputCol("label")
    val qd = new QuantileDiscretizer()
      .setNumBuckets(5)
      .setInputCol("real2")
      .setOutputCol("real2_buckets")

    val vaInputs = collection.mutable.ArrayBuffer[String]("real1", "real2_buckets")

    val catIndexers = Seq("cat1", "cat2").map { col =>
      val outputCol = s"${col}_idx"
      vaInputs += outputCol
      new StringIndexer().setInputCol(col).setOutputCol(outputCol)
    }

    val assembler = new VectorAssembler().setInputCols(vaInputs.toArray).setOutputCol("assembled")

    val selector = new ChiSqSelector()
      .setFeaturesCol("assembled")
      .setOutputCol("features")
      .setNumTopFeatures(3)

    val stages: Seq[PipelineStage] = Seq(qd) ++ catIndexers :+ assembler :+ labelIndexer :+ selector
    stages
  }

  def makePipeline(additionalStages: PipelineStage*) = {
    val pipeline = new Pipeline()
    pipeline.setStages((featureStages ++ additionalStages).toArray)
  }

  val clf = new LogisticRegression()
  val pipeline = makePipeline(clf)

  override val sparkTransformer = pipeline.fit(data)
  val sparkOutput = sparkTransformer.transform(data)
  override val input = data.toJSON.collect()
  override val expectedOutput = withColumnAsArray(sparkOutput,
    "assembled", clf.getFeaturesCol).toJSON.collect()

  test("Additional classifiers") {
    Seq(
      new LogisticRegression().setFitIntercept(false),
      new DecisionTreeClassifier(),
      new RandomForestClassifier().setNumTrees(5),
      new GBTClassifier().setMaxIter(5),
      new LinearSVC().setRegParam(0.001).setMaxIter(20),
      new LinearSVC().setRegParam(0.001).setFitIntercept(false).setMaxIter(20),
      new MultilayerPerceptronClassifier().setLayers(Array(3, 4, 2))
    ).foreach { clf =>
      val pipeline = makePipeline(clf)
      val sparkTransformer = pipeline.fit(data)
      val result = sparkTransformer.transform(data)
      val expectedOutput = withColumnAsArray(result,
        "assembled", clf.getFeaturesCol).toJSON.collect()

      parityTest(sparkTransformer, input, expectedOutput)
    }
  }

  test("Regressors") {
    Seq(
      new LinearRegression(),
      new LinearRegression().setFitIntercept(false),
      new DecisionTreeRegressor(),
      new RandomForestRegressor().setNumTrees(5),
      new GBTRegressor().setMaxIter(5)
    ).foreach { clf =>
      val pipeline = makePipeline(clf)
      val sparkTransformer = pipeline.fit(data)
      val result = sparkTransformer.transform(data)
      val expectedOutput = withColumnAsArray(result,
        "assembled", clf.getFeaturesCol).toJSON.collect()

      parityTest(sparkTransformer, input, expectedOutput)
    }
  }
}

case class PipelineResult(
  label_str: String,
  cat1: String,
  cat2: String,
  real1: Double,
  real2: Double,
  real2_buckets: Double,
  cat1_idx: Double,
  cat2_idx: Double,
  label: Double,
  assembled: Seq[Double],
  features: Seq[Double],
  prediction: Double) extends Result