import org.scalatest._


class SpatialFilter$Test extends FlatSpec with Matchers {

  "a row with coordinates" should "return true" in {
    val row = Map("dwc:decimalLatitude" -> "32.5", "dwc:decimalLongitude" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe true
  }

  "a row with malformed coordinates" should "return false" in {
    val row = Map("dwc:decimalLatitude" -> "john", "dwc:decimalLongitude" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe false
  }

  "a row without coordinates" should "return false" in {
    val row = Map("dwc:donals" -> "32.5", "dwc:mickey" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe false
  }

  "a row with coordinates outside" should "return false" in {
    val row = Map("dwc:donals" -> "32.5", "dwc:mickey" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe false
  }


}
