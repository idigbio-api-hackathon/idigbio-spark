object MycoBank {

  def generateId(fields: Map[String, String]): String = {
    val values = List("dwc:scientificName", "dwc:scientificNameAuthorship") flatMap (fields get)
    "myco:" ++ values.mkString(" ").trim
  }

}
