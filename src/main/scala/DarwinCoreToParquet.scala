import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import scopt._
import java.net.URL
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import DwC.Meta

import scala.IllegalArgumentException

trait DwCHandler {
  def toDF(metas: Seq[String]): Seq[(String, DataFrame)]
}

trait DwCSparkHandler extends DwCHandler {
  implicit var sqlContext: SQLContext

  def toDF(metaLocators: Seq[String]): Seq[(String, DataFrame)] = {
    val metaURLs: Seq[URL] = metaLocators map { meta => new URL(meta) }
    val metas: Seq[DwC.Meta] = metaURLs flatMap { metaURL: URL => DwC.readMeta(metaURL) }
    val metaDFTuples = metas map { meta: DwC.Meta =>
      val schema = StructType(meta.coreTerms map {
        StructField(_, StringType, true)
      })
      meta.fileLocations map { fileLocation =>
        println(s"attempting to load [$fileLocation]...")
        val df = sqlContext.read.format("com.databricks.spark.csv")
          .option("delimiter", meta.delimiter)
          .schema(schema)
          .load(fileLocation.toString)
        (fileLocation, df.except(df.limit(meta.skipHeaderLines)))
      }
    }
    metaDFTuples.flatten
  }

  def toLinkDF(occurrenceDF: DataFrame, columnNames: List[String]): DataFrame = {
    def escapeColumnName(name: String): String = {
      Seq("`", name, "`").mkString("")
    }

    val externalIdColumns = occurrenceDF.schema.
      filter(_.dataType == org.apache.spark.sql.types.StringType).
      map(_.name).
      filter(columnNames.contains(_)).
      map(escapeColumnName)

    val idsOnly = occurrenceDF.select(externalIdColumns.head, externalIdColumns.tail: _*)
    val links = idsOnly
      .flatMap(row => (2 to row.length).toSeq.map(index => Row(row.getString(0), "refers", row.getString(index - 1))))
      .filter(row => row.getString(2).nonEmpty)


    val linkSchema =
      StructType(
        Seq("start_id", "link_rel", "end_id").map(fieldName => StructField(fieldName, StringType, true)))

    sqlContext.createDataFrame(links, linkSchema)
  }
}

object DarwinCoreToParquet extends DwCSparkHandler {
  implicit var sc: SparkContext = _
  implicit var sqlContext: SQLContext = _

  case class Config(archives: Seq[String] = Seq())

  def main(args: Array[String]) {
    config(args) match {
      case Some(config) => {
        val conf = new SparkConf()
          .setAppName("dwc2parquet")
        sqlContext = new SQLContext(new SparkContext(conf))
        for ((sourceLocation, df) <- toDF(config.archives)) {
          df.write.format("parquet").save(sourceLocation + ".parquet")
        }
      }
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def config(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("dwcToParquet") {
      head("dwcToParquet", "0.x")
      arg[String]("<dwc meta.xml url> ...") unbounded() required() action { (x, c) =>
        c.copy(archives = c.archives :+ x)
      } text ("list of darwin core archives")
    }

    parser.parse(args, Config())
  }

}
