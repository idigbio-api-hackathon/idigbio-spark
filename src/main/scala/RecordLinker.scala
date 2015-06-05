import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RecordLinker {
  def main(args: Array[String]) {
    val occurrenceFile = args(0)

    def recordLinkBuilder(linkerName: String): (Map[String, String] => String) = linkerName match {
      case "globalnames" => GlobalNamesUUID.generateGlobalNamesUUID
      case "genbank" => GenBank.generateId
      case _ => NameConcat.concatName

    }

    val conf = new SparkConf().setAppName("iDigBio-LD")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(occurrenceFile).cache()
    val headers = new CSVParser().parseLine(logData.take(1).head)

    val links: RDD[(String, String)] = handleLines(logData, headers, "id", recordLinkBuilder(args(1)))
    links.map(link => List(link._1,link._2).mkString(",")).saveAsTextFile(occurrenceFile + ".links")

  }

  def handleLines(lines: RDD[String],
                  headers: Seq[String],
                  idColumnLabel: String,
                  generateLink: (Map[String, String]) => String) = {
    val rowList = lines
      .flatMap(RecordLinker.parseLine)
      .map(fields => headers zip fields)

    rowList.map { row =>
      linkRecord(row.toMap, idColumnLabel, generateLink)
    }
  }


  def linkRecord(row: Map[String, String], idColumnLabel: String, generateLink: (Map[String, String]) => String) = {
    val ids = row.filter(tuple => {
      tuple._1 == idColumnLabel
    })

    (ids.head._2, generateLink(row))
  }

  def parseLine(lineString: String): Option[Array[String]] = {
    try {
      val parser: CSVParser = new CSVParser()
      Some(parser.parseLine(lineString))
    } catch {
      case e: Exception =>
        None
    }
  }


}
