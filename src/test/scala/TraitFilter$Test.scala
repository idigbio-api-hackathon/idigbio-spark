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

  "row with exact trait value match but measurement mismatch" should "return true" in {
    val traitSelector = Map("Measurement URI" -> "boo baa"
      , "Units URI (normalized)" -> "http://purl.obolibrary.org/obo/UO_0000009"
      , "values" -> "123")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "123"
    )) shouldBe false
  }

  "row with exact trait match" should "return true" in {
    val traitSelector = measurementAndUnit ++ Map("values" -> "123")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "123"
    )) shouldBe true
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
