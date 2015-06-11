name := "iDigBio-LD"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0-M1",
  "org.littleshoot" % "littleshoot-commons-id" % "1.0.3",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "com.spatial4j" % "spatial4j" % "0.4.1",
  "org.scalatest" % "scalatest_2.10" % "2.2.5" % "test"
)
