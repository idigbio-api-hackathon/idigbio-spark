import scala.util.Success
import scala.util.parsing.combinator._

object TraitFilter {

  def unit: String = "Units URI (normalized)"

  def measurement = "Measurement URI"

  def measurementAndUnit = List(measurement, unit)

  def value = "Value"

  def valueOptions = "values"

  def minValue = "minValue"

  def maxValue = "maxValue"

  def compatibleNumericValueFor(record: Map[String, String], toUnit: String): Option[BigDecimal] = {
    (record.get(value), record.get(unit)) match {
      case (Some(aValue), Some(fromUnit)) =>
        conversionFactor(fromUnit, toUnit) match {
          case Some(aFactor) =>
            try {
              Some(aFactor * BigDecimal(aValue.replace(",", "")))
            } catch {
              case _: NumberFormatException => None
            }
          case _ =>
            None
        }
      case _ => None
    }
  }


  def conversionFactor(fromUnit: String, toUnit: String): Option[BigDecimal] = {
    val conversions = Map(
      "http://purl.obolibrary.org/obo/UO_0000009->http://purl.obolibrary.org/obo/UO_0000021" -> BigDecimal(1000.0)
      , "http://purl.obolibrary.org/obo/UO_0000021->http://purl.obolibrary.org/obo/UO_0000009" -> BigDecimal(1.0 / 1000.0))

    val factor =
      conversions.get(Seq(fromUnit, toUnit).mkString("->")) match {
        case Some(aFactor) =>
          Some(aFactor)
        case _ =>
          if (toUnit == fromUnit) {
            Some(BigDecimal(1.0))
          } else {
            None
          }
      }
    factor
  }

  def hasTraits(traitSelectors: Seq[String], record: Map[String, String]): Boolean = {
    traitSelectors
      .map(parseTraitConfig)
      .forall(hasTrait(_, record))
  }

  def hasTrait(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    val traitMatchers = Seq(
      compatibleMeasurementAndUnit(traitSelector, record),
      valueInOptions(traitSelector, record),
      valueLessThan(traitSelector, record),
      valueGreaterThan(traitSelector, record)
    )
    traitMatchers.forall(_ == true)
  }

  def valueGreaterThan(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(minValue), compatibleNumericValueFor(record, traitSelector.getOrElse(unit, ""))) match {
      case (Some(minValueSelector), Some(aValue)) =>
        aValue > BigDecimal(minValueSelector)
      case (None, _) =>
        true
      case (_, _) =>
        false
    }
  }

  def valueLessThan(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(maxValue), compatibleNumericValueFor(record, traitSelector.getOrElse(unit, ""))) match {
      case (Some(maxValueSelector), Some(aValue)) =>
        aValue < BigDecimal(maxValueSelector)
      case (None, _) => true
      case _ => false
    }
  }

  def valueInOptions(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(valueOptions), compatibleNumericValueFor(record, traitSelector.getOrElse(unit, ""))) match {
      case (Some(valueSelector), Some(aValue)) =>
        val options = splitOptions(valueSelector)
        options.map(BigDecimal(_)).contains(aValue)
      case (Some(valueSelector), None) =>
        (record.get(value), record.get(unit), traitSelector.get(unit)) match {
          case (Some(aValue), None, None) =>
            splitOptions(valueSelector).contains(aValue)
          case (Some(aValue), Some(aUnit), None) =>
            splitOptions(valueSelector).contains(aValue)
          case _ => false
        }
      case (None, _) => true
      case _ => false
    }
  }

  def splitOptions(valueSelector: String): Array[String] = {
    valueSelector.split( """\|""")
  }

  def compatibleMeasurementAndUnit(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    record.get(measurement).equals(traitSelector.get(measurement))
  }

  def parseTraitConfig(traitFilterConfig: String): Map[String, String] = {
    TraitFilterConfigParser.parse(TraitFilterConfigParser.config, traitFilterConfig) match {
      case TraitFilterConfigParser.Success(traitConfig, _) => traitConfig
      case TraitFilterConfigParser.Failure(msg, _) => Map()
      case TraitFilterConfigParser.Error(msg, _) => Map()
    }
  }

}
