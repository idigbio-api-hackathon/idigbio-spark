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

trait LinkIdentifiers {
  implicit var sqlContext: SQLContext

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
    val links = idsOnly.
      flatMap(row => (2 to row.length).toSeq.map(index => Row(row.getString(0), "refers", row.getString(index - 1)))).
      filter(row => {
        val endId = row.getString(2)
        endId != null && endId.nonEmpty
      })


    val linkSchema =
      StructType(
        Seq("start_id", "link_rel", "end_id").map(fieldName => StructField(fieldName, StringType)))

    sqlContext.createDataFrame(links, linkSchema)
  }
}

object LinkIdentifiersJob extends LinkIdentifiers {
  implicit var sc: SparkContext = _
  implicit var sqlContext: SQLContext = _

  case class Config(archives: Seq[String] = Seq(), outputFile: String = "not defined", outputFormat: String = "parquet")

  def main(args: Array[String]) {
    config(args) match {
      case Some(config) => {
        val conf = new SparkConf()
          .setAppName("linkIdentifiers")
        sqlContext = new SQLContext(new SparkContext(conf))
        val linkDFs = config.archives map { archive =>
          println(s"parquet file at [$archive] processing...")
          val df = sqlContext.read.format("parquet").load(archive)
          val coreIdName: String = df.columns.head
          val linkDF = toLinkDF(df, coreIdName :: IdentifierUtil.dwcIdColumns)
          println(s"parquet file at [$archive] processed.")
          linkDF
        }
        val unionLinkDF = linkDFs.reduce((res, linkDF) => res.unionAll(linkDF))
        println(s"write links to [${config.outputFile}] processing...")
        unionLinkDF.write.format(config.outputFormat).save(config.outputFile)
        println(s"write links to [${config.outputFile}] done.")
      }
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def config(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("linkIdentifiers") {
      head("linkIdentifiers", "0.x")
      opt[String]('o', "output") required() valueName "<output path>" action { (x, c) =>
          c.copy(outputFile = x) } text "output path"
      opt[String]('f', "output-format") optional() valueName "<format>" action { (x, c) =>
          c.copy(outputFormat = x) } text "output format"
      arg[String]("<parquet url> ...") unbounded() required() action { (x, c) =>
        c.copy(archives = c.archives :+ x)
      } text "list of darwin core parquet archives"
    }

    parser.parse(args, Config())
  }

}
