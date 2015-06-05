name := "iDigBio-LD"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.littleshoot" % "littleshoot-commons-id" % "1.0.3",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "org.scalatest" % "scalatest_2.10" % "2.2.5" % "test"
)
