name := "iDigBio-LD"

version := "1.5.1"

scalaVersion := "2.10.5"

val sparkV: String = "1.6.0"

libraryDependencies ++= Seq(
  "org.littleshoot" % "littleshoot-commons-id" % "1.0.3" excludeAll (
    ExclusionRule("org.slf4j", "slf4j-api")),
  "com.google.guava" % "guava" % "16.0.1",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "org.apache.commons" % "commons-csv" % "1.1",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.databricks" % "spark-csv_2.10" % "1.3.0",
  "org.globalnames" %% "gnparser" % "0.2.0" excludeAll(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "org.scala-lang")),
  "org.locationtech.spatial4j" % "spatial4j" % "0.6",
  "com.vividsolutions" % "jts-core" % "1.14.0",
  "org.apache.spark" %% "spark-sql" % sparkV % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkV % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1" excludeAll(
    ExclusionRule(organization = "io.netty"),
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("com.google.guava", "guava")),

  "org.scalatest" %% "scalatest" % "2.2.5" % "test"
)

test in assembly := {}

resolvers += Resolver.sonatypeRepo("public")
