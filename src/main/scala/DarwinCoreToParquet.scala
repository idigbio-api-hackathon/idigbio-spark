import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import scopt._
import java.net.URL
import DwC.Meta

import scala.IllegalArgumentException

trait DwCHandler {
  def handle(metas: Seq[String])
}

trait DwCSparkHandler extends DwCHandler {
  implicit var sqlContext: SQLContext

  def handle(metas: Seq[String]) {
    val metaURLs: Seq[URL] = metas map { meta => new URL(meta) }
    val coreDFs = metaURLs map { metaURL: URL =>
      DwC.readMeta(metaURL) match {
        case Some(meta) => {
          val schema = StructType(meta.coreTerms map { StructField(_, StringType, true) } )
          meta.fileLocations map { fileLocation =>
            println(s"attempting to load [$fileLocation]...")
            sqlContext.read.format("com.databricks.spark.csv")
              .option("delimiter", meta.delimiter)
              .schema(schema) 
              .load(fileLocation.toString)        
          }          
        }
        case None => { 
          println(s"nothing readable from [$metaURL]")
          Seq(sqlContext.emptyDataFrame)
        }
      }
    }
    
    val dfs: Seq[DataFrame] = coreDFs.flatten
    val intersectSchema = dfs map { df => df.columns }
    // should merge schemas here somewhere
    
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
          .set("spark.cassandra.connection.host", "localhost")
          .setAppName("DwCToParquet")
        sqlContext = new SQLContext(new SparkContext(conf))

        handle(config.archives)        
      }
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def config(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("dwcToParquet") {
      head("dwcToParquet", "0.x")
      arg[String]("<dwc meta.xml url> ...") unbounded () required () action { (x, c) =>
        c.copy(archives = c.archives :+ x)
      } text ("list of darwin core archives")
    }

    parser.parse(args, Config())
  }

}
