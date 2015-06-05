import org.scalatest._

class GlobalNamesUUID$Test extends FlatSpec with Matchers {

  "A line from csv file" should "be parse in good order" in {
    GlobalNamesUUID.hashName("Homo sapiens") should be("16f235a0-e4a3-529c-9b83-bd15fe722110")
    GlobalNamesUUID.hashName("Crinoidea") should be("813583ad-c364-5c15-b01a-43eaa1446fee")
    GlobalNamesUUID.hashName("Anolis roquet majolgris x Anolis roquet summus Lazell, 1972") should be("f03c977f-b56e-52e4-b36c-885ebf4fd61b")
  }

  "give a bag" should "create a gn uuid" in {
    val gnUUID: String = GlobalNamesUUID.generateGlobalNamesUUID(Map("dwc:scientificName" -> "Crinoidea", "dwc:scientificNameAuthority" -> ""))
    gnUUID should be("gn:813583ad-c364-5c15-b01a-43eaa1446fee")
  }



}
