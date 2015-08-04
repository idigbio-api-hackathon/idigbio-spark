import org.scalatest._

class TraitFilter$Test extends FlatSpec with Matchers {

  val measurementAndUnit = Map(
    "Measurement URI" -> "http://purl.obolibrary.org/obo/VT_0001259",
    "Units URI (normalized)" -> "http://purl.obolibrary.org/obo/UO_0000009")


  "row with exact trait value match but unit mismatch" should "return true" in {
    val traitSelector = Map("Measurement URI" -> "http://purl.obolibrary.org/obo/VT_0001259"
      , "Units URI (normalized)" -> "boo baa"
      , "values" -> "123")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "123"
    )) shouldBe false
  }

  "row with exact trait value match with compatible but different unit" should "return true" in {
    val traitSelector = Map("Measurement URI" -> "http://purl.obolibrary.org/obo/VT_0001259"
      , "Units URI (normalized)" -> "http://purl.obolibrary.org/obo/UO_0000021"
      , "values" -> "123000")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "123")) shouldBe true
  }

  "row with exact trait value match included in list but with compatible but different unit" should "return true" in {
    val traitSelector = Map("Measurement URI" -> "http://purl.obolibrary.org/obo/VT_0001259"
      , "Units URI (normalized)" -> "http://purl.obolibrary.org/obo/UO_0000021"
      , "values" -> "444|123000")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "123")) shouldBe true
    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "0.444")) shouldBe true
  }

  "row with exact trait value match but measurement mismatch" should "return true" in {
    val traitSelector = Map("Measurement URI" -> "boo baa"
      , "Units URI (normalized)" -> "http://purl.obolibrary.org/obo/UO_0000009"
      , "values" -> "123")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "123")) shouldBe false
  }

  "row with exact trait match different unit" should "return true" in {
    val traitSelector = measurementAndUnit ++ Map("values" -> "123")
    val record: Map[String, String] = measurementAndUnit ++ Map("Value" -> "123000", "Units URI (normalized)" -> "http://purl.obolibrary.org/obo/UO_0000021")
    TraitFilter.hasTrait(traitSelector, record) shouldBe true
  }

  "row with exact trait match" should "return true" in {
    val traitSelector = measurementAndUnit ++ Map("values" -> "123")
    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "123")) shouldBe true
  }

  "row with trait mismatch" should "return false" in {
    val traitSelector = measurementAndUnit ++ Map("values" -> "444")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map(
      "Value" -> "123"
    )) shouldBe false
  }

  "row with trait less than" should "return false" in {
    val traitSelector = measurementAndUnit ++ Map("maxValue" -> "444")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map(
      "Value" -> "123"
    )) shouldBe true
  }

  "row with trait out of range less than" should "return false" in {
    val traitSelector = measurementAndUnit ++ Map("minValue" -> "444")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map(
      "Value" -> "123"
    )) shouldBe false
  }

  "row with trait greater than" should "return false" in {
    val traitSelector = measurementAndUnit ++ Map("maxValue" -> "444")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map(
      "Value" -> "123"
    )) shouldBe true
  }

  "row with trait out of range greater than" should "return false" in {
    val traitSelector = measurementAndUnit ++ Map("maxValue" -> "444")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "666"
    )) shouldBe false
  }

}
