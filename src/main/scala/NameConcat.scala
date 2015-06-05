

object NameConcat {

  def concatName(row: Map[String, String]): String = {
    val values: List[String] = List("dwc:scientificName", "dwc:scientificNameAuthorship") flatMap (row get)
    values.mkString(" ")
  }

}
