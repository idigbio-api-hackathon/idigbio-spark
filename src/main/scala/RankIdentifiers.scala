import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.spark.graphx.Graph
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

trait RankIdentifiers {
  implicit var sqlContext: SQLContext

  def toRankDF(linkDFs: Seq[RDD[Row]]): RDD[(Double, String)] = {
    val links = linkDFs.reduce((res, linkDF) => res.union(linkDF))
    val edges = links.map(row => IdentifierUtil.toEdge(row))
    val vertices = links.flatMap(row => IdentifierUtil.toVertices(row)).distinct

    val idGraph = Graph(vertices, edges, "nothing")
    val ranks = idGraph.pageRank(0.0001).vertices
    val ranksByVertex = vertices.join(ranks).map {
      case (id, (vertex, rank)) => (rank, vertex)
    }
    ranksByVertex.sortByKey(ascending = false)
  }
}

object RankIdentifiersJob extends RankIdentifiers {
  implicit var sc: SparkContext = _
  implicit var sqlContext: SQLContext = _

  case class Config(archives: Seq[String] = Seq()
                    , outputFile: String = "not defined"
                    , outputFormat: String = "parquet"
                    , inputFormat: String = "parquet"
                    , limit: Int = 1000)

  def main(args: Array[String]) {
    config(args) match {
      case Some(config) => {
        val conf = new SparkConf()
          .setAppName("rankIdentifiers")
        sqlContext = new SQLContext(new SparkContext(conf))
        val linkDFs = config.archives map { archive =>
          println(s"archive at [$archive] loading...")
          val df = sqlContext.read.format(config.inputFormat).load(archive)
          println(s"archive at [$archive] loaded")
          df.rdd
        }
        val rankRDD = toRankDF(linkDFs)

        println(s"write link ranks to [${config.outputFile}] processing...")
        val rankDF = sqlContext.createDataFrame(rankRDD)
        rankDF.limit(config.limit).write.format(config.outputFormat).save(config.outputFile)
        println(s"write link ranks to [${config.outputFile}] done.")
      }
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def config(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("rankIdentifiers") {
      head("rankIdentifiers", "0.x")
      opt[String]('o', "output") required() valueName "<output path>" action { (x, c) =>
        c.copy(outputFile = x)
      } text "output path"
      opt[String]('f', "output-format") optional() valueName "<format>" action { (x, c) =>
        c.copy(outputFormat = x)
      } text "output format"
      opt[String]('i', "input-format") optional() valueName "<format>" action { (x, c) =>
        c.copy(inputFormat = x)
      } text "input format"
      opt[Int]('l', "limit") optional() valueName "<limit>" action { (x, c) =>
        c.copy(limit = x)
      } text "maximum number of sorted ranked ids to be saved"
      arg[String]("<link archive url> ...") unbounded() required() action { (x, c) =>
        c.copy(archives = c.archives :+ x)
      } text "list of three column archives with structure start_id,rel,end_id"
    }

    parser.parse(args, Config())
  }

}
