name := "PaytmPhderome"

version := "1.0"

scalaVersion := "2.11.8"

// installed spark 2.1.0 (with hadoop 2.7.2) locally on computer
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
)