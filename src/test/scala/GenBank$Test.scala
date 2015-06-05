import org.scalatest._

class GenBank$Test extends FlatSpec with Matchers {


  "give a bag with genbank ids" should "create a genbank id" in {
    val gnUUID: String = GenBank.generateId(Map("dwc:associatedSequences" -> "GenBank FJ266907 (cytb) GenBank FJ267193 (ND4)", "dwc:scientificNameAuthority" -> ""))
    gnUUID should be("genbank:FJ266907|FJ267193")
  }

  "give a bag with genbank urls" should "create a genbank id" in {
    val gnUUID: String = GenBank.generateId(Map("dwc:associatedSequences" -> "http://www.ncbi.nlm.nih.gov/nuccore/AF285919 ; http://www.ncbi.nlm.nih.gov/nuccore/AF285941", "dwc:scientificNameAuthority" -> ""))
    gnUUID should be("genbank:AF285919|AF285941")
  }

  "give a bag with commas" should "create a genbank id" in {
    val gnUUID: String = GenBank.generateId(Map("dwc:associatedSequences" -> ", , , , , , ,", "dwc:scientificNameAuthority" -> ""))
    gnUUID should be("")
  }



}
