import au.com.bytecode.opencsv.CSVParser
import org.scalatest._

class RecordLinker$Test extends FlatSpec with Matchers {

  "A line from csv file" should "be parse in good order" in {
    val parser = new CSVParser(',')
    val singleLine = parser.parseLine( """one,two,three""")
    singleLine(0) should be( """one""")
    singleLine(1) should be( """two""")
  }

  "invalid line" should "be ignored" in {
    val lineString: String = """h1,"h2,h3"""
    RecordLinker.parseLine(lineString) should be(None)
  }



  def columnConcatLinker(row: Map[String, String]): String = {
    val values: List[String] = List("h2", "h3") flatMap (row get)
    values.mkString(" ")
  }


}
