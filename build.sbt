name := "iDigBio-LD"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0" % "provided" exclude("com.google.guava", "guava"),
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M1" exclude("com.google.guava", "guava"),
  "org.littleshoot" % "littleshoot-commons-id" % "1.0.3",
  "com.google.guava" % "guava" % "16.0.1",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "com.spatial4j" % "spatial4j" % "0.4.1",
  "org.scalatest" % "scalatest_2.10" % "2.2.5" % "test"
)
