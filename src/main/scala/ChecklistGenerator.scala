import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._


object ChecklistGenerator {
  def main(args: Array[String]) {
    if (args.length < 3) {
      throw new IllegalArgumentException("usage: checklist [occurrence file] [taxon selector] [wktString] [cassandra]")
    }

    val occurrenceFile = args(0)
    val taxonSelector = args(1).split( """[\|,]""").toList
    val wktString = args(2).trim

    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("iDigBio-LD")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(occurrenceFile).cache()
    val headers = new CSVParser().parseLine(logData.take(1).head)

    val rows = readRows(headers, logData)



    val filtered = applySpatioTaxonomicFilter(rows, taxonSelector, wktString)
    val sortedChecklist = countByTaxonAndSort(filtered)

    val output = if (args.length > 3) args(3).trim else ""
    val taxonSelectorString: String = taxonSelector.mkString("|")
    output match {
      case "cassandra" => {
        CassandraConnector(sc.getConf).withSessionDo { session =>
          session.execute(CassandraUtil.checklistKeySpaceCreate)
          session.execute(CassandraUtil.checklistRegistryTableCreate)
          session.execute(CassandraUtil.checklistTableCreate)
        }
        sortedChecklist.cache().map(item => (taxonSelectorString, wktString, item._1, item._2))
          .saveToCassandra("idigbio", "checklist", CassandraUtil.checklistColumns)

        sc.parallelize(Seq((taxonSelectorString, wktString, "ready", sortedChecklist.count())))
          .saveToCassandra("idigbio", "checklist_registry", CassandraUtil.checklistRegistryColumns)
      }

      case _ => sortedChecklist.map(item => List(taxonSelectorString, wktString, item._1, item._2).mkString(","))
        .saveAsTextFile(occurrenceFile + ".checklist" + System.currentTimeMillis)
    }

  }

  def countByTaxonAndSort(rowList: RDD[Seq[(String, String)]]): RDD[(String, Int)] = {
    rowList.map(row => (TaxonFilter.taxonFields
      .flatMap(row.toMap get).mkString("|"), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
  }

  def applySpatioTaxonomicFilter(rows:  RDD[Seq[(String, String)]], taxonSelector: List[String], wktString: String, traitSelector: List[String] = List()): RDD[Seq[(String, String)]] = {
    rows
      .filter(row => TaxonFilter.hasTaxa(taxonSelector, row.toMap))
      .filter(row => SpatialFilter.locatedIn(wktString, row.toMap))
  }


  def readRows(headers: Seq[String], rdd: RDD[String]): RDD[Seq[(String, String)]] = {
    rdd
      .flatMap(RecordLinker.parseLine)
      .map(fields => headers zip fields)
  }

  def filterByTraits(checklist: RDD[(String, Int)], traits: RDD[Seq[(String, String)]], traitSelectors: Seq[String]): RDD[(String, Int)] = {
      if (traitSelectors.isEmpty) {
        checklist
      } else {
        val selectedNamesByTraitsRDD = traits
          .filter(record => TraitFilter.hasTraits(traitSelectors, record.toMap))
          .map(record => {
          record.find(_._1 == "Scientific Name") match {
            case Some((_, aName)) => (aName.trim, 1)
            case _ => ("", 1)
          }
        }).distinct().filter(_._1.nonEmpty)

        val keyedChecklistRDD = checklist.map(item => (item._1.split( """\|""").reverse.head.trim, (item._1, item._2)))

        val checklistMatchingTraits = keyedChecklistRDD
          .join(selectedNamesByTraitsRDD)
          .map(item => item._2._1)
        checklistMatchingTraits
      }
    }
}
