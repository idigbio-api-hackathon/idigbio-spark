import org.scalatest._

class TaxonFilter$Test extends FlatSpec with Matchers {

  "a row with matching taxon" should "return true" in {
    TaxonFilter.hasTaxa(List("Homo sapiens"), Map("dwc:scientificName" -> "Homo sapiens")) shouldBe true
  }

  "a row with matching genus" should "return true" in {
    TaxonFilter.hasTaxa(List("Homo"), Map("dwc:scientificName" -> "Homo sapiens", "dwc:genus" -> "Homo")) shouldBe true
  }

  "a row with matching kingdom" should "return true" in {
    TaxonFilter.hasTaxa(List("Animalia"), Map("dwc:scientificName" -> "Homo sapiens", "dwc:kingdom" -> "Animalia")) shouldBe true
  }

  "a row with no matches genus" should "return true" in {
    TaxonFilter.hasTaxa(List("Ariopsis"), Map("dwc:scientificName" -> "Homo sapiens", "dwc:genus" -> "Homo")) shouldBe false
  }

  "a row with no matches fields" should "return true" in {
    TaxonFilter.hasTaxa(List("Homo"), Map("dwc:donald" -> "Homo sapiens", "dwc:duck" -> "Homo")) shouldBe false
  }

}
