import scala.collection.immutable.StringOps

object GenBank {

  def generateId(fields: Map[String, String]): String = {
    val idPattern = """[a-zA-Z]{1,2}\-?_?\d{5,6}""".r
    val values = List("dwc:associatedSequences") flatMap (fields get)
    val associatedSequences: StringOps = values.mkString("").trim
    val genBankIds = (idPattern findAllIn associatedSequences).toList
    genBankIds match {
      case Nil => ""
      case ids => "genbank:" ++ ids.mkString("|")

    }
  }

}
