import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

import scala.IllegalArgumentException


object ChecklistGenerator {
  def main(args: Array[String]) {
    if (args.length < 6) {
      throw new IllegalArgumentException("usage: checklist [occurrence file] [taxon selector] [wktString] [cassandra] [trait selector] [traits file]")
    }

    val occurrenceFile = args(0)
    val taxonSelector = args(1).split( """[\|,]""").toList
    val taxonSelectorString: String = taxonSelector.mkString("|")
    val wktString = args(2).trim
    val output = args(3).trim

    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("iDigBio-LD")
    val sc = new SparkContext(conf)
    val occurrences: RDD[Seq[(String, String)]] = parseCSV(occurrenceFile, sc)

    val traitSelectors = args(4).trim.split( """[\|,]""").toSeq.filter(_.nonEmpty)
    val traitSelectorString: String = traitSelectors.mkString("|")
    val traitsFile = args(5).trim

    val traits: RDD[Seq[(String, String)]] = parseCSV(traitsFile, sc)

    val filtered = applySpatioTaxonomicFilter(occurrences, taxonSelector, wktString)
    val checklist = filterByTraits(countByTaxonAndSort(filtered), traits, traitSelectors)

    output match {
      case "cassandra" => {
        CassandraConnector(sc.getConf).withSessionDo { session =>
          session.execute(CassandraUtil.checklistKeySpaceCreate)
          session.execute(CassandraUtil.checklistRegistryTableCreate)
          session.execute(CassandraUtil.checklistTableCreate)
        }
        checklist.cache().map(item => (taxonSelectorString, wktString, traitSelectorString, item._1, item._2))
          .saveToCassandra("effechecka", "checklist", CassandraUtil.checklistColumns)

        sc.parallelize(Seq((taxonSelectorString, wktString, traitSelectorString, "ready", checklist.count())))
          .saveToCassandra("effechecka", "checklist_registry", CassandraUtil.checklistRegistryColumns)
      }

      case _ => checklist.map(item => List(taxonSelectorString, wktString, traitSelectorString, item._1, item._2).mkString(","))
        .saveAsTextFile(occurrenceFile + ".checklist" + System.currentTimeMillis)
    }

  }

  def parseCSV(csvFile: String, sc: SparkContext): RDD[Seq[(String, String)]] = {
    val lines = sc.textFile(csvFile).cache()
    val headers = new CSVParser().parseLine(lines.take(1).head)
    readRows(headers, lines)
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
