import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.joda.time.{Interval, DateTime, DateTimeZone}

object OccurrenceCollectionGenerator {

  def generateCollection(config: ChecklistConf) {
    val occurrenceFile = config.occurrenceFiles.head
    val taxonSelector = config.taxonSelector
    val taxonSelectorString: String = taxonSelector.mkString("|")
    val wktString = config.geoSpatialSelector.trim

    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setAppName("occ2collection")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContextSingleton.getInstance(sc)
    val occurrences: DataFrame = sqlContext.read.format("parquet").load(occurrenceFile)
    val occurrenceCollection = OccurrenceCollectionBuilder
      .buildOccurrenceCollection(sc, occurrences, wktString, taxonSelector)

    val traitSelectors = config.traitSelector
    val traitSelectorString: String = traitSelectors.mkString("|")

    config.outputFormat.trim match {
      case "cassandra" => {
        CassandraConnector(sc.getConf).withSessionDo { session =>
          session.execute(CassandraUtil.checklistKeySpaceCreate)
          session.execute(CassandraUtil.occurrenceCollectionRegistryTableCreate)
          session.execute(CassandraUtil.occurrenceCollectionTableCreate)
        }
        occurrenceCollection.map(item => (taxonSelectorString, wktString, traitSelectorString, item._3, item._1, item._2, item._7, item._8, item._4, item._5, item._6))
          .saveToCassandra("effechecka", "occurrence_collection", CassandraUtil.occurrenceCollectionColumns)

        sc.parallelize(Seq((taxonSelectorString, wktString, traitSelectorString, "ready", occurrenceCollection.count())))
          .saveToCassandra("effechecka", "occurrence_collection_registry", CassandraUtil.occurrenceCollectionRegistryColumns)
      }

      case _ => occurrenceCollection.map(item => List(taxonSelectorString, wktString, traitSelectorString, item._1, item._2).mkString(","))
        .saveAsTextFile(occurrenceFile + ".occurrences" + System.currentTimeMillis)
    }

  }

  def main(args: Array[String]) {
    config(args) match {
      case Some(c) =>
        generateCollection(c)
      case _ =>
    }
  }

  def config(args: Array[String]): Option[ChecklistConf] = {

    def splitAndClean(arg: String): Seq[String] = {
      arg.trim.split( """[\|,]""").toSeq.filter(_.nonEmpty)
    }

    val parser = new scopt.OptionParser[ChecklistConf]("occ2collection") {
      head("occ2collection", "0.x")
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

object OccurrenceCollectionBuilder {
  def buildOccurrenceCollection(sc: SparkContext, df: DataFrame, wkt: String, taxa: Seq[String]): RDD[(String, String, String, String, String, String, Long, Long)] = {
    val sqlContext: SQLContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._

    val locationTerms = List("`http://rs.tdwg.org/dwc/terms/decimalLatitude`"
      , "`http://rs.tdwg.org/dwc/terms/decimalLongitude`")
    val taxonNameTerms = List("kingdom", "phylum", "class", "order", "family", "genus", "specificEpithet", "scientificName")
      .map(term => s"`http://rs.tdwg.org/dwc/terms/$term`")
    val eventDateTerm = "`http://rs.tdwg.org/dwc/terms/eventDate`"

    val remainingTerms = List(eventDateTerm, "`http://rs.tdwg.org/dwc/terms/occurrenceID`", "`date`", "`source`")

    val availableTerms: Seq[String] = (locationTerms ::: taxonNameTerms ::: remainingTerms) intersect df.columns.map(_.mkString("`", "", "`"))
    val availableTaxonTerms = taxonNameTerms.intersect(availableTerms)


    import org.apache.spark.sql.functions.udf
    val hasDate = udf(DateUtil.validDate(_: String))
    val startDateOf = udf(DateUtil.startDate(_: String))
    val endDateOf = udf(DateUtil.endDate(_: String))
    val taxaSelected = udf((taxonPath: String) => taxa.intersect(taxonPath.split("\\|")).nonEmpty)
    val locationSelected = udf((lat: String, lng: String) => {
      SpatialFilter.locatedInLatLng(wkt, Seq(lat, lng))
    })

    if (availableTerms.containsSlice(locationTerms)
      && availableTerms.containsSlice(remainingTerms)
      && availableTaxonTerms.nonEmpty) {
      val taxonPathTerm: String = "taxonPath"
      val withPath = df.select(availableTerms.map(col): _*)
        .withColumn(taxonPathTerm, concat_ws("|", availableTaxonTerms.map(col): _*))

      val occColumns = locationTerms ::: List(taxonPathTerm) ::: remainingTerms
      withPath.select(occColumns.map(col): _*)
        .filter(hasDate(col("date")))
        .filter(hasDate(col(eventDateTerm)))
        .filter(taxaSelected(col(taxonPathTerm)))
        .filter(locationSelected(locationTerms.map(col): _*))
        .withColumn("pdate", startDateOf(col("date")))
        .withColumn("psource", col("source"))
        .withColumn("start", startDateOf(col(eventDateTerm)))
        .withColumn("end", endDateOf(col(eventDateTerm)))
        .drop(col(eventDateTerm))
        .drop(col("date"))
        .drop(col("source"))
        .as[(String, String, String, String, String, String, Long, Long)]
        .rdd
    } else {
      sc.emptyRDD[(String, String, String, String, String, String, Long, Long)]
    }
  }


}


