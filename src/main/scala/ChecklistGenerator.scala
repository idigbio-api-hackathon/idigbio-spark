import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ChecklistGenerator {
  def main(args: Array[String]) {
    if (args.length < 3) {
      throw new IllegalArgumentException("usage: checklist [occurrence file] [taxon selector] [wktString]")
    }

    val occurrenceFile = args(0)
    val taxonSelector = args(1).split("""\|""").toList
    val wktString = args(2).trim

    val conf = new SparkConf().setAppName("iDigBio-LD")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(occurrenceFile).cache()
    val headers = new CSVParser().parseLine(logData.take(1).head)

    val filtered = applySpatialTaxonomicFilter(headers, logData, taxonSelector, wktString)
    val sortedChecklist = countByTaxonAndSort(filtered)
    sortedChecklist.map(item => List(item._1, item._2).mkString(",")).saveAsTextFile(occurrenceFile + ".checklist" + System.currentTimeMillis)
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

  def countByTaxonAndSort(rowList: RDD[Seq[(String, String)]]): RDD[(String, Int)] = {
    val checklist = rowList.map(row => (List("dwc:scientificName")
      .flatMap(row.toMap get).mkString("|"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
    checklist
  }

  def applySpatialTaxonomicFilter(headers: Seq[String], rdd: RDD[String], taxonSelector: List[String], wktString: String): RDD[Seq[(String, String)]] = {
    val rowList = rdd
      .flatMap(RecordLinker.parseLine)
      .map(fields => headers zip fields)
      .filter(row => TaxonFilter.hasTaxa(taxonSelector, row.toMap))
      .filter(row => SpatialFilter.locatedIn(wktString, row.toMap))
    rowList
  }


}
