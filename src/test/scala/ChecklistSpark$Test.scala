import com.spatial4j.core.context.SpatialContext
import com.spatial4j.core.shape.{Shape, SpatialRelation}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class ChecklistSpark$Test extends FlatSpec with BeforeAndAfter with Matchers {

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
      .applySpatialTaxonomicFilter(headers, rdd, List("Mickey mousus", "Mini mousus", "Donald duckus"), "ENVELOPE(10,21,13,10)")

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


}
