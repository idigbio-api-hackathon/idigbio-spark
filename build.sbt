name := "iDigBio-LD"

version := "1.1"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.littleshoot" % "littleshoot-commons-id" % "1.0.3" excludeAll (
    ExclusionRule("org.slf4j", "slf4j-api")),
  "com.google.guava" % "guava" % "16.0.1",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "com.spatial4j" % "spatial4j" % "0.4.1",
  "org.apache.spark" %% "spark-core" % "1.5.0" % "provided" excludeAll(
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("com.google.guava", "guava")),

  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M1" excludeAll(
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("com.google.guava", "guava")),

  "org.scalatest" % "scalatest_2.10" % "2.2.5" % "test"
)

test in assembly := {}