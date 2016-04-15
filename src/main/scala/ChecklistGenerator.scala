import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.globalnames.parser.ScientificNameParser.{instance => snp}
import org.json4s._

object SQLContextSingleton {
  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

case class ChecklistConf(occurrenceFiles: Seq[String] = Seq()
                         , traitFiles: Seq[String] = Seq()
                         , traitSelector: Seq[String] = Seq()
                         , taxonSelector: Seq[String] = Seq()
                         , geoSpatialSelector: String = ""
                         , outputFormat: String = "cassandra"
                         , firstSeenOnly: Boolean = true)

object ChecklistGenerator {

  def generateChecklist(config: ChecklistConf) {
    val occurrenceFile = config.occurrenceFiles.head
    val taxonSelector = config.taxonSelector
    val taxonSelectorString: String = taxonSelector.mkString("|")
    val wktString = config.geoSpatialSelector.trim

    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("occ2checklist")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContextSingleton.getInstance(sc)
    val occurrences: DataFrame = sqlContext.read.format("parquet").load(occurrenceFile)
    val occChecklist = ChecklistBuilder.buildChecklist(sc, occurrences, wktString, taxonSelector)

    val traitSelectors = config.traitSelector
    val traitSelectorString: String = traitSelectors.mkString("|")
    val traitsFile = config.traitFiles.head.trim

    val traits: RDD[Seq[(String, String)]] = parseCSV(traitsFile, sc)

    val checklist = filterByTraits(occChecklist, traits, traitSelectors)

    config.outputFormat.trim match {
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

  def applySpatioTaxonomicFilter(rows: RDD[Seq[(String, String)]], taxonSelector: List[String], wktString: String, traitSelector: List[String] = List()): RDD[Seq[(String, String)]] = {
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

      val keyedChecklistRDD = checklist.map(item => {
        val lastNameInTaxonPath: String = item._1.split( """\|""").last.trim
        val scientificName = snp.fromString(lastNameInTaxonPath)
        val nameForMatching = scientificName
          .canonized(showRanks = false)
          .getOrElse(lastNameInTaxonPath)
        (nameForMatching, (item._1, item._2))
      })

      val checklistMatchingTraits = keyedChecklistRDD
        .join(selectedNamesByTraitsRDD)
        .map(item => item._2._1)
      checklistMatchingTraits
    }
  }

  def main(args: Array[String]) {
    config(args) match {
      case Some(c) =>
        generateChecklist(c)
      case _ =>
    }
  }

  def config(args: Array[String]): Option[ChecklistConf] = {

    def splitAndClean(arg: String): Seq[String] = {
      arg.trim.split( """[\|,]""").toSeq.filter(_.nonEmpty)
    }

    val parser = new scopt.OptionParser[ChecklistConf]("occ2checklist") {
      head("occ2checklist", "0.x")
      opt[String]('f', "output-format") optional() valueName "<output format>" action { (x, c) =>
        c.copy(outputFormat = x)
      } text "output format"
      opt[String]('c', "<occurrence url>") required() action { (x, c) =>
        c.copy(occurrenceFiles = splitAndClean(x))
      } text "list of occurrence archive urls"
      opt[String]('t', "<traits url>") required() action { (x, c) =>
        c.copy(traitFiles = splitAndClean(x))
      } text "list of trait archive urls"

      arg[String]("<taxon selectors>") required() action { (x, c) =>
        c.copy(taxonSelector = splitAndClean(x))
      } text "pipe separated list of taxon names"
      arg[String]("<geospatial selector>") required() action { (x, c) =>
        c.copy(geoSpatialSelector = x.trim)
      } text "WKT string specifying an geospatial area of interest"
      arg[String]("trait selectors") optional() action { (x, c) =>
        c.copy(traitSelector = splitAndClean(x))
      } text "pipe separated list of trait criteria"
    }

    parser.parse(args, ChecklistConf())
  }
}

object ChecklistBuilder {
  def buildChecklist(sc: SparkContext, df: DataFrame, wkt: String, taxa: Seq[String]): RDD[(String, Int)] = {
    val sqlContext: SQLContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._

    val locationTerms = List("`http://rs.tdwg.org/dwc/terms/decimalLatitude`"
      , "`http://rs.tdwg.org/dwc/terms/decimalLongitude`")
    val taxonNameTerms = List("kingdom", "phylum", "class", "order", "family", "genus", "specificEpithet", "scientificName")
      .map(term => s"`http://rs.tdwg.org/dwc/terms/$term`")

    val availableTerms: Seq[String] = (locationTerms ::: taxonNameTerms) intersect df.columns.map(_.mkString("`", "", "`"))
    val availableTaxonTerms = taxonNameTerms.intersect(availableTerms)

    if (availableTerms.containsSlice(locationTerms) && availableTaxonTerms.nonEmpty) {
      val withPath = df.select(availableTerms.map(col): _*)
        .withColumn("taxonPath", concat_ws("|", availableTaxonTerms.map(col): _*))
      withPath.select(locationTerms.head, locationTerms.last, "taxonPath")
        .as[(String, String, String)]
        .filter(p => taxa.intersect(p._3.split("\\|")).nonEmpty)
        .filter(p => SpatialFilter.locatedInLatLng(wkt, Seq(p._1, p._2)))
        .map(p => (p._3, 1))
        .rdd.reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
    } else {
      sc.emptyRDD[(String, Int)]
    }
  }
}


