name := "aardpfark"

organization := "com.ibm"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.7.0",
  "com.sksamuel.avro4s" %% "avro4s-json" % "1.7.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "test",
  "org.apache.spark" %% "spark-hive" % "2.2.0" % "test",
  "com.opendatagroup" % "hadrian" % "0.8.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.2" % "test"
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false