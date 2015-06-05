import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class RecordLinkerSpark$Test extends FlatSpec with BeforeAndAfter with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "filter merge header and line" should "return selected value" in {
    val headers = Seq("id", "dwc:scientificName", "dwc:scientificNameAuthorship", "dwc:someOther")
    val lines = Seq("123,Mickey mousus,walt,xyz", "345,Donald duckus,walt,zzz")
    val rdd = sc.parallelize(lines)

    val recordLinks = RecordLinker.handleLines(rdd, headers, "id", NameConcat.concatName).collect()

    recordLinks should contain(("345", "Donald duckus walt"))
    recordLinks should contain(("123", "Mickey mousus walt"))
    recordLinks should not contain (("h2", "v2_2"))
    recordLinks should not contain (("h2", "v2_1"))
  }

}
