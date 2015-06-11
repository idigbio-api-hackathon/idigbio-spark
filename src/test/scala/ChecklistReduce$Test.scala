import org.scalatest._

import scala.util.Try

class ChecklistReduce$Test extends FlatSpec with Matchers {

  def parseDouble(s: String): Option[Double] = Try {
    s.toDouble
  }.toOption

  def hasTaxa(taxa: List[String], record: Map[String, String]): Boolean = {
    List("dwc:scientificName", "dwc:kingdom", "dwc:phylum", "dwc:order", "dwc:class", "dwc:family", "dwc:genus")
      .flatMap (record get)
      .filter(taxa.contains(_)).nonEmpty
  }

  "a row with matching taxon" should "return true" in {
    assert(hasTaxa(List("Homo sapiens"), Map("dwc:scientificName" -> "Homo sapiens")))
  }

  "a row with matching genus" should "return true" in {
    assert(hasTaxa(List("Homo"), Map("dwc:scientificName" -> "Homo sapiens", "dwc:genus" -> "Homo")))
  }

  "a row with no matches genus" should "return true" in {
    hasTaxa(List("Ariopsis"), Map("dwc:scientificName" -> "Homo sapiens", "dwc:genus" -> "Homo")) shouldBe false
  }

  "a row with no matches fields" should "return true" in {
    hasTaxa(List("Homo"), Map("dwc:donald" -> "Homo sapiens", "dwc:duck" -> "Homo")) shouldBe false
  }

}
