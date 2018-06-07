[![Build Status](https://travis-ci.org/CODAIT/aardpfark.svg?branch=master)](https://travis-ci.org/CODAIT/aardpfark)

# Aardpfark

Aardpfark is a library for exporting Spark ML models and pipelines to the [Portable Format for Analytics (PFA)](http://dmg.org/pfa/).

PFA is a JSON format for representing machine learning models, data transformations and analytic applications.
The format encapsulates both serialization as well as the operations (or functions) to be applied to
input data to produce output data. It can essentially be thought of as a mini functional language, 
together with a data schema specification.

A PFA ["document"](http://dmg.org/pfa/docs/document_structure/) is fully self-contained and can be executed by any
compliant execution engine, making a model written to PFA truly portable across languages, frameworks, and runtimes.

# Installation

## Prerequisites

* [`sbt`](https://www.scala-sbt.org/)
* [Apache Maven](https://maven.apache.org/) for [installing](#running-the-tests) test dependency
* [Apache Spark](https://spark.apache.org/)

## Quick start

Aardpfark currently targets and has been tested on Apache Spark 2.2.0. 2.3.0 support will be added soon.

1. Build the `aardpfark` project (ignoring tests ) using `sbt 'set test in assembly := {}' clean assembly`
2. Add the aardpfark JAR to your Spark application, e.g. using spark-shell:

```
./bin/spark-shell --driver-class-path /PATH_TO_AARDPFARK_JAR/aardpfark-assembly-0.1.0-SNAPSHOT.jar

```

### Use with SBT or Maven

*Note* publishing to Maven coming soon.

First you will need to install `aardpfark` locally using `sbt publish-local`. Then, add it to your SBT build file:

```scala
libraryDependencies += "com.ibm" %% "aardpfark" % "0.1.0-SNAPSHOT"
```


## Usage

Aardpfark provides functions for exporting supported models to PFA as JSON strings. For example,
to export a simple logistic regression model and print the resulting PFA document:


```scala
import com.ibm.aardpfark.spark.ml.SparkSupport.toPFA

import org.apache.spark.ml.classification._

val data = spark.read.format("libsvm").load("data/sample_multiclass_classification_data.txt")
val lr = new LogisticRegression()
val model = lr.fit(data)

val pfa = toPFA(model, true)
println(pfa)

```

### Pipeline support

Aardpfark also supports exporting pipeline consisting of supported models and transformers. Because 
it requires access to the schema information of the input dataframe, you must also pass in that schema
to the export function:

```scala
import com.ibm.aardpfark.spark.ml.SparkSupport.toPFA

import org.apache.spark.ml._
import org.apache.spark.ml.feature._

val data = spark.read.format("libsvm").load("data/sample_multiclass_classification_data.txt")
val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaled")
val lr = new LogisticRegression().setFeaturesCol("scaled")
val pipeline = new Pipeline().setStages(Array(scaler, lr))
val model = pipeline.fit(data)

val pfa = toPFA(model, data.schema, true)
println(pfa)

```

Support for more natural implicit conversions is also in progress.

## Scoring exported models

To score exported models, use a reference PFA scoring engine in Java, Python or R from the
[Hadrian project](https://github.com/opendatagroup/hadrian). *Note* for the JVM engine you will 
need to install the `daily` branch build (see the [instructions below](#running-the-tests)).

For example, using the Hadrian JVM engine (in Scala). You can add the Hadrian jar to the driver classpath

```
$SPARK_HOME/bin/spark-shell --driver-class-path /PATH_TO_AARDPFARK_JAR/aardpfark-assembly-0.1.0-SNAPSHOT.jar:/PATH_TO_HADRIAN_JAR/hadrian-0.8.5.jar
```

and execute the following:

```scala
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import com.ibm.aardpfark.spark.ml.SparkSupport.toPFA

import org.apache.spark.ml.classification._

val data = spark.read.format("libsvm").load("data/sample_multiclass_classification_data.txt")
val lr = new LogisticRegression()
val model = lr.fit(data)

val pfa = toPFA(model, true)
val engine = PFAEngine.fromJson(pfa, multiplicity = 1).head
val input = """{"features":[-0.222222,0.5,-0.762712,-0.833333]}"""
println(engine.action(engine.jsonInput(input)))
```

You should see the result returned as JSON:

```json
{
   "rawPrediction":[
      -80.61228861915214,
      100.66271325935413,
      -20.050424640201975
   ],
   "prediction":1.0,
   "probability":[
      1.8761474921138084E-79,
      1.0,
      3.7579441119545976E-53
   ]
}
```

Check out the aardpfark test cases to see further examples. We are working on adding more detailed 
examples and benchmarks.

## Running the tests

*Note* `aardpfark` tests depend on the JVM reference implementation of a PFA scoring engine: [Hadrian]().
Hadrian has not yet published a version supporting Scala 2.11 to Maven, so you will need to install the 
`daily` branch to run the tests.

Install Hadrian using the following steps:

1. Clone the repo: `git clone https://github.com/opendatagroup/hadrian.git`
2. Change to the cloned `hadrian` sub-directory: `cd hadrian/hadrian`
3. Checkout the `daily` branch: `git checkout daily`
4. Install locally using Maven: `mvn install`

Run tests using `sbt test`. The test cases include checking equivalence between what Spark ML components produce and
what PFA produces.

# Coverage

Aardpfark aims to provide complete coverage of all Spark ML components. The current coverage status
is listed below.

**NOTE** export to PFA is for Models and Transformers only (not Estimators)

| Component | Status |
| --- | --- |
| _**Predictors**_ |  |
| Logistic Regression | Supported |
| LinearSVC | Supported |
| Linear Regression| Supported |
| Generalized Linear Model | Supported |
| Multilayer Perceptron | Supported |
| Decision Tree Classifier & Regressor | Supported |
| Gradient Boosted Tree Classifier & Regressor | Supported |
| Naive Bayes | Supported |
| OneVsRest | Not yet |
| AFTSurvivalRegresstion | Not yet |
| IsotonicRegression | Not yet |
| _**Clustering**_ |  |
| KMeans | Supported |
| Bisecting KMeans | Not yet |
| LDA | Not yet |
| Gaussian Mixture| Not yet |
| _**Recommendations**_ |  |
| ALS | Not yet |
| _**Feature Extractors**_ |  |
| CountVectorizerModel | Supported |
| IDFModel | Supported |
| Word2Vec | Not yet |
| HashingTF | Not yet |
| FeatureHasher | Not yet |
| _**Feature Transformers**_ |  |
| Binarizer | Supported |
| Bucketizer | Supported |
| ElementwiseProduct | Supported |
| MaxAbsScalerModel | Supported |
| MinMaxScalerModel | Supported |
| NGram | Supported |
| Normalizer | Supported |
| PCAModel | Supported |
| QuantileDiscretizer | Supported |
| RegexTokenizer | Supported |
| StandardScalerModel | Supported |
| StopWordsRemover| Supported |
| StringIndexerModel | Supported |
| VectorAssembler | Supported |
| OneHotEncoderModel | Not yet |
| PolynomialExpansion | Not yet |
| IndexToString | Not yet |
| VectorIndexer | Not yet |
| Imputer | Not yet |
| Interaction | Not yet |
| VectorSizeHint | Won't support (TBD) |
| DCT | Won't support |
| SQLTransformer | Won't support |
| _**Feature Selectors**_ |  |
| ChiSqSelectorModel | Supported |
| VectorSlicer | Supported |
| RFormula | Not yet |
| _**LSH**_ |  |
| LSH transformers | Not yet |


# Roadmap

Immediate objectives include:
* Complete adding support for Spark ML components together with tests
* Complete Scala DSL and tests
* Add PySpark support
* Improve the existing test coverage
* Improve the pipeline support

Longer term objectives include:

* Add support for other ML libraries, starting with scikit-learn
* Add support for generic vectors (mixed sparse/dense)


# Contributing

We welcome contributions - whether it be adding or improving documentation and examples, adding support 
for missing Spark ML components, or any other item on the roadmap above. See [CONTRIBUTING](CONTRIBUTING.md)
for details and open an issue or pull request.

# License

Aardpfark is released under an [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0.html) (see [LICENSE](LICENSE)).
