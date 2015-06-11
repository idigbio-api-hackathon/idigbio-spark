import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

class SparkJobs$Test extends FlatSpec with BeforeAndAfterAll with Matchers {

  private var sc: SparkContext = _

  override def beforeAll() = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "localhost")
      .setMaster("local[2]")
      .setAppName("test-spark")
    sc = new SparkContext(conf)
  }

  override def afterAll() = {
    if (sc != null) {
      sc.stop()
    }
  }

  "concatenating rows" should "link the record with the concatenated values" in {
    val headers = Seq("id", "dwc:scientificName", "dwc:scientificNameAuthorship", "dwc:someOther")
    val lines = Seq("123,Mickey mousus,walt,xyz", "345,Donald duckus,walt,zzz")
    val rdd = sc.parallelize(lines)

    val recordLinks = RecordLinker.handleLines(rdd, headers, "id", NameConcat.concatName).collect()

    recordLinks should contain(("345", "Donald duckus walt"))
    recordLinks should contain(("123", "Mickey mousus walt"))
    recordLinks should not contain (("h2", "v2_2"))
    recordLinks should not contain (("h2", "v2_1"))
  }

  "generating a checklist" should "an ordered list of most frequently observed taxa" in {
    val headers = Seq("id", "dwc:scientificName", "dwc:scientificNameAuthorship", "dwc:decimalLatitude", "dwc:decimalLongitude")
    val lines = Seq("123,Mickey mousus,walt,12.2,16.4"
      , "234,Mickey mousus,walt,12.1,17.7"
      , "234,Mickey mousus,walt,32.2,16.7"
      , "345,Donald duckus,walt,12.2,16.7"
      , "345,Donald duckus,walt,12.2,16.7"
      , "345,Donald duckus,walt,12.2,16.7"
      , "345,Donald duckus,walt,112.2,16.7"
      , "345,Donald duckus,walt,112.2,16.7"
      , "401,Mini mousus,walt,12.02,16.2")

    val rdd = sc.parallelize(lines)

    val rowList: RDD[Seq[(String, String)]] = ChecklistGenerator
      .applySpatioTaxonomicFilter(headers, rdd, List("Mickey mousus", "Mini mousus", "Donald duckus"), "ENVELOPE(10,21,13,10)")

    rowList.collect should have length 6

    val checklist: RDD[(String, Int)] = ChecklistGenerator.countByTaxonAndSort(rowList)

    val checklistTop2: Array[(String, Int)] = checklist.take(2)
    checklistTop2 should have length 2
    checklistTop2 should contain("Mickey mousus", 2)
    checklistTop2 should not(contain("Mini mousus", 1))
    checklistTop2.head should be("Donald duckus", 3)

    val checklistAll = checklist.collect()
    checklistAll should have length 3
    checklistAll should contain("Mini mousus", 1)
  }

  // cassandra can run embedded, but has a library dependency conflict with spark
    @Ignore def `"concatenating rows" should "be saved to cassandra" in` {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(CassandraUtil.checklistKeySpaceCreate)
      session.execute(CassandraUtil.checklistTableCreate)
      session.execute(s"TRUNCATE idigbio.checklist")
    }
    val otherLines = Seq(("Mammalia|Insecta", "LINE(1 2 3 4)", "checklist item", 1)
      , ("Mammalia|Insecta", "LINE(1 2 3 4)", "other checklist item", 1))

    val rdd = sc.parallelize(otherLines)
      .saveToCassandra("idigbio", "checklist", CassandraUtil.checklistColumns)

  }


}
