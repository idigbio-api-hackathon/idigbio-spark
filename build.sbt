name := "iDigBio-LD"

version := "1.3.2"

scalaVersion := "2.10.5"

val sparkV: String = "1.6.0"
libraryDependencies ++= Seq(
  "org.littleshoot" % "littleshoot-commons-id" % "1.0.3" excludeAll (
    ExclusionRule("org.slf4j", "slf4j-api")),
  "com.google.guava" % "guava" % "16.0.1",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "org.apache.commons" % "commons-csv" % "1.1",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.databricks" % "spark-csv_2.10" % "1.3.0",
  "com.spatial4j" % "spatial4j" % "0.4.1",
  "org.apache.spark" %% "spark-sql" % sparkV % "provided" excludeAll(
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("com.google.guava", "guava")),
  "org.apache.spark" %% "spark-graphx" % sparkV % "provided" excludeAll(
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("com.google.guava", "guava")),
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M1" excludeAll(
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("com.google.guava", "guava")),

  "org.scalatest" %% "scalatest" % "2.2.5" % "test"
)

test in assembly := {}

resolvers += Resolver.sonatypeRepo("public")
