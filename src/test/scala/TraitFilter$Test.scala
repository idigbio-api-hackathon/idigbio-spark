import au.com.bytecode.opencsv.CSVParser
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

  "big number row with trait greater of equal than" should "return true" in {
    val traitSelector = measurementAndUnit ++ Map("minValue" -> "10")

    val aRecord = Map(
      "Value" -> "154321.3045"
      , "Measurement URI" -> "http://purl.obolibrary.org/obo/VT_0001259"
      , "Units URI (normalized)" -> "http://purl.obolibrary.org/obo/UO_0000009"
    )

    TraitFilter.hasTrait(traitSelector, aRecord) shouldBe true
  }

  "row with trait out of range greater than" should "return false" in {
    val traitSelector = measurementAndUnit ++ Map("maxValue" -> "444")

    TraitFilter.hasTrait(traitSelector, measurementAndUnit ++ Map("Value" -> "666"
    )) shouldBe false
  }

  "parse greaterThan trait config" should "produce valid filters" in {
    val expectedTraitFilterConfig = Map(
      """Measurement URI""" -> """http://purl.obolibrary.org/obo/VT_0001259""",
      """Units URI (normalized)""" -> """http://purl.obolibrary.org/obo/UO_0000021""",
      """minValue""" -> """1200"""
    )

    TraitFilter.parseTraitConfig( """bodyMass greaterThan 1200 g""") should be(expectedTraitFilterConfig)
  }

  "parse greaterThan trait config" should "produce valid filters with kg" in {
    val expectedTraitFilterConfig = Map(
      """Measurement URI""" -> """http://purl.obolibrary.org/obo/VT_0001259""",
      """Units URI (normalized)""" -> """http://purl.obolibrary.org/obo/UO_0000009""",
      """minValue""" -> """1200"""
    )

    TraitFilter.parseTraitConfig( """bodyMass greaterThan 1200 kg""") should be(expectedTraitFilterConfig)
  }

  "parse lessThan trait config" should "produce valid filters" in {
    val expectedTraitFilterConfig = Map(
      """Measurement URI""" -> """http://purl.obolibrary.org/obo/VT_0001259""",
      """Units URI (normalized)""" -> """http://purl.obolibrary.org/obo/UO_0000021""",
      """maxValue""" -> """1200"""
    )

    TraitFilter.parseTraitConfig( """bodyMass lessThan 1200 g""") should be(expectedTraitFilterConfig)
  }

  "parse lessThan trait config kg" should "produce valid filters" in {
    val expectedTraitFilterConfig = Map(
      """Measurement URI""" -> """http://purl.obolibrary.org/obo/VT_0001259""",
      """Units URI (normalized)""" -> """http://purl.obolibrary.org/obo/UO_0000009""",
      """maxValue""" -> """1200"""
    )

    TraitFilter.parseTraitConfig( """bodyMass lessThan 1200 kg""") should be(expectedTraitFilterConfig)
  }

  "list of traits for taxon" should "be filtered using trait filter" in {
    val aRecord: Map[String, String] = getRecord

    TraitFilter.hasTrait(TraitFilter.parseTraitConfig( """bodyMass greaterThan 1200 g"""), aRecord) shouldBe true
    TraitFilter.hasTrait(TraitFilter.parseTraitConfig( """bodyMass greaterThan 1200 kg"""), aRecord) shouldBe true
    TraitFilter.hasTrait(TraitFilter.parseTraitConfig( """bodyMass greaterThan 220000 kg"""), aRecord) shouldBe false


    TraitFilter.hasTraits(List( """bodyMass greaterThan 1200 g""", """bodyMass greaterThan 220000 kg"""), aRecord) shouldBe false


    //val mismatchingTraitDescriptors: List[String] = List( """bodyMass greaterOrEqualTo 1200 g""", """bodyMass lessOrEqualTo 25 g""")
    //TraitFilter.hasTraits(mismatchingTraitDescriptors, aRecord) shouldBe false
  }

  "record" should "not be selected of any of the traits is not present" in {
    val aRecord: Map[String, String] = getRecord
    TraitFilter.hasTraits(List( """bodyMass greaterThan 1200 g""", """bodyMass greaterThan 2400 kg"""), aRecord) shouldBe true
  }

  "record" should "not be selected lessThan trait does not match" in {
    TraitFilter.hasTraits(List( """bodyMass lessThan 2400 kg"""), getRecord) shouldBe false
  }

  "record" should "be selected greaterThan body mass in kg trait matches" in {
    TraitFilter.hasTrait(TraitFilter.parseTraitConfig("""bodyMass greaterThan 2400 kg"""), getRecord) shouldBe true
  }

  "record" should "be selected equal body mass value string matches" in {
    TraitFilter.hasTrait(TraitFilter.parseTraitConfig("""bodyMass in 12|154321.3045"""), getRecord) shouldBe true
  }

  "record" should "be selected equal body mass value in kg string matches" in {
    TraitFilter.hasTrait(TraitFilter.parseTraitConfig("""bodyMass equals 154321.3045 kg"""), getRecord) shouldBe true
  }

  "record" should "not be selected equal body mass value string matches" in {
    TraitFilter.hasTrait(TraitFilter.parseTraitConfig("""bodyMass equals 154321.30450"""), getRecord) shouldBe false
  }

  "record" should "be selected equal body mass value in kg string matches inspite of trailing zero" in {
    TraitFilter.hasTrait(TraitFilter.parseTraitConfig("""bodyMass equals 154321.30450 kg"""), getRecord) shouldBe true
  }

  def getRecord: Map[String, String] = {
    val headers = """EOL page ID,Scientific Name,Common Name,Measurement,Value,Measurement URI,Value URI,Units (normalized),Units URI (normalized),Raw Value (direct from source),Raw Units (direct from source),Raw Units URI (normalized),Supplier,Content Partner Resource URL,source,citation,measurement method,statistical method,life stage,scientific name,measurement remarks,Reference,contributor"""
    val fourLines = Seq( """328574,Balaenoptera musculus,Blue Whale,body mass,154321.3045,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,154321304.5,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaenoptera musculus,,,"""
      , """328577,Balaena mysticetus,Bowhead Whale,body mass,79691.17899,http://purl.obolibrary.org/obo/VT_0001259,"",kg,http://purl.obolibrary.org/obo/UO_0000009,79691178.98999999,g,http://purl.obolibrary.org/obo/UO_0000021,PanTHERIA,http://eol.org/content_partners/652/resources/704,"Data set supplied by Kate E. Jones. The data can also be accessed at Ecological Archives E090-184-D1, <a href=""http://esapubs.org/archive/ecol/E090/184/"">http://esapubs.org/archive/ecol/E090/184/</a>, <a href=""http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt"">http://esapubs.org/archive/ecol/E090/184/PanTHERIA_1-0_WR05_Aug2008.txt</a>","Kate E. Jones, Jon Bielby, Marcel Cardillo, Susanne A. Fritz, Justin O'Dell, C. David L. Orme, Kamran Safi, Wes Sechrest, Elizabeth H. Boakes, Chris Carbone, Christina Connolly, Michael J. Cutts, Janine K. Foster, Richard Grenyer, Michael Habib, Christopher A. Plaster, Samantha A. Price, Elizabeth A. Rigby, Janna Rist, Amber Teacher, Olaf R. P. Bininda-Emonds, John L. Gittleman, Georgina M. Mace, and Andy Purvis. 2009. PanTHERIA: a species-level database of life history, ecology, and geography of extant and recently extinct mammals. Ecology 90:2648.","Mass of adult (or age unspecified) live or freshly-killed specimens (excluding pregnant females) using captive, wild, provisioned, or unspecified populations; male, female, or sex unspecified individuals; primary, secondary, or extrapolated sources; all measures of central tendency; in all localities. Based on information from primary and secondary literature sources. See source for details.",average,adult,Balaena mysticetus,,,"""
      , """222044,Sargochromis carlottae,Rainbow Happy,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=5364"">http://www.fishbase.org/summary/SpeciesSummary.php?id=5364</a>",,,max,,,,"Skelton, P.H.0 A complete guide to the freshwater fishes of southern Africa. Southern Book Publishers. 388 p. (Ref. 7248)",Susan M. Luna"""
      , """1003713,Netuma thalassina,Giant Catfish,body mass,"1,000",http://purl.obolibrary.org/obo/VT_0001259,"",g,http://purl.obolibrary.org/obo/UO_0000021,"1,000",g,http://purl.obolibrary.org/obo/UO_0000021,FishBase,http://eol.org/content_partners/2/resources/42,"<a href=""http://www.fishbase.org/summary/SpeciesSummary.php?id=10220"">http://www.fishbase.org/summary/SpeciesSummary.php?id=10220</a>",,,max,,"Netuma thalassina (Rüppell, 1837)",,"Bykov, V.P.0 Marine Fishes: Chemical composition and processing properties. New Delhi: Amerind Publishing Co. Pvt. Ltd. 322 p. (Ref. 4883)",Pascualita Sa-a""")
    val header = new CSVParser().parseLine(headers)
    val firstLine = new CSVParser().parseLine(fourLines.head)
    val aRecord: Map[String, String] = (header.toSeq zip firstLine).toMap
    aRecord
  }
}
