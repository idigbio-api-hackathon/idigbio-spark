import scala.util.parsing.combinator._

object TraitFilterConfigParser extends RegexParsers {
  val measurementMap = Map("bodyMass" -> "http://purl.obolibrary.org/obo/VT_0001259")
  val operatorMap = Map("greaterThan" -> "minValue"
    , ">" -> "minValue"
    , "lessThan" -> "maxValue"
    , "<" -> "maxValue"
    , "in" -> "values"
    , "==" -> "values"
    , "=" -> "values"
    , "equals" -> "values")
  val unitMap = Map("g" -> "http://purl.obolibrary.org/obo/UO_0000021", "kg" -> "http://purl.obolibrary.org/obo/UO_0000009")

  def term: Parser[String] = """[^\s]+""".r ^^ {
    _.toString
  }

  def config: Parser[Map[String, String]] = term ~ term ~ term ~ rep(term) ^^ {
    case measurement ~ operator ~ value ~ tail =>
      val filterConfig = Map("Measurement URI" -> measurementMap.getOrElse(measurement, measurement)
        , operatorMap.getOrElse(operator, operator) -> value)
      if (tail.nonEmpty) {
        filterConfig ++ Map("Units URI (normalized)" -> unitMap.getOrElse(tail.head, tail.head))
      } else {
        filterConfig
      }
  }
}