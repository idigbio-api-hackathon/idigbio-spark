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

    val filtered = applySpatioTaxonomicFilter(headers, logData, taxonSelector, wktString)
    val sortedChecklist = countByTaxonAndSort(filtered)
    sortedChecklist.map(item => List(item._1, item._2).mkString(",")).saveAsTextFile(occurrenceFile + ".checklist" + System.currentTimeMillis)
  }

  def countByTaxonAndSort(rowList: RDD[Seq[(String, String)]]): RDD[(String, Int)] = {
    rowList.map(row => (TaxonFilter.taxonFields
      .flatMap(row.toMap get).mkString("|"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
  }

  def applySpatioTaxonomicFilter(headers: Seq[String], rdd: RDD[String], taxonSelector: List[String], wktString: String): RDD[Seq[(String, String)]] = {
    rdd
      .flatMap(RecordLinker.parseLine)
      .map(fields => headers zip fields)
      .filter(row => TaxonFilter.hasTaxa(taxonSelector, row.toMap))
      .filter(row => SpatialFilter.locatedIn(wktString, row.toMap))
  }


}
