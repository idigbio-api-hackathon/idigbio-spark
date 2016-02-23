import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

class IdentifierUtil$Test extends FlatSpec with Matchers with BeforeAndAfterAll {
  
  "a row" should "be transformed into a list of links" in {
    val aRow = Row.fromTuple ("one", "two", "three")

  }


}
