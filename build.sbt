name := "scala-spark-machine-learning"

version := "0.1"

scalaVersion := "2.11.12"

// Scala dependencies
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.12"

// Spark dependencies
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"

// N-Dimensional Arrays for Java dependencies
//libraryDependencies += "org.nd4j" % "nd4j-x86" % "0.4-rc3.8" % Test

//libraryDependencies += "org.nd4j" % "nd4j-jcublas-7.0" % "0.4-rc3.8" % Test

//libraryDependencies += "org.nd4j" % "nd4j-api" % "0.4-rc3.8" % Test

// Deeplearning4j dependencies
//libraryDependencies += "org.deeplearning4j" % "dl4j-spark" % "0.4-rc3.8" % Test

// Typesafe dependencies
libraryDependencies += "com.typesafe" % "config" % "1.3.3"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
