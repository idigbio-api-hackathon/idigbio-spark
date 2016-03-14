import org.scalatest._


class SpatialFilter$Test extends FlatSpec with Matchers {

  "a row with coordinates in envelope" should "return true" in {
    val row = Map("dwc:decimalLatitude" -> "32.5", "dwc:decimalLongitude" -> "12.3")
    SpatialFilter.locatedIn("ENVELOPE(10,13,40,30)", row) shouldBe true
  }

  "a row with coordinates in wrapped envelope" should "return true" in {
    val row = Map("dwc:decimalLatitude" -> "32.5", "dwc:decimalLongitude" -> "120")
    SpatialFilter.locatedIn("ENVELOPE(106,76,81,7)", row) shouldBe true
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

  "an envelope covering the arctic" should "include a point on the arctic" in {
    val row = Map("dwc:decimalLatitude" -> "89.5", "dwc:decimalLongitude" -> "20.0")
    SpatialFilter.locatedIn("ENVELOPE(0.703125,-0.703125,90,65.07213008560697)", row) shouldBe true
  }

}
