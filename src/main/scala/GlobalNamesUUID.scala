import org.apache.commons.id.uuid.UUID

object GlobalNamesUUID {

  def generateGlobalNamesUUID(fields: Map[String, String]): String = {
    val values = List("dwc:scientificName", "dwc:scientificNameAuthorship") flatMap (fields get)
    "gn:" ++ hashName(values.mkString(" ").trim)
  }

  def hashName(name: String): String = {
    val gn = UUID.fromString("90181196-fecf-5082-a4c1-411d4f314cda")
    val version3: String = UUID.nameUUIDFromString(name, gn, "SHA1").toString
    version3.substring(0, 14) ++ "5" ++ version3.substring(15, version3.length)
  }

}
