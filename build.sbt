name := "PaytmPhderome"

version := "1.0"

scalaVersion := "2.11.8"

mainClass in assembly := Some("phderome.challenge.WeblogChallenge")

assemblyJarName in assembly := "paytmphderome_2.11-1.0.jar"

// installed spark 2.1.0 (with hadoop 2.7.2) locally on computer
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.1.0"  % Provided,
  "org.scalacheck" %% "scalacheck" % "1.13.4" % Test,
  "org.typelevel"    %% "cats"       % "0.7.2" )
