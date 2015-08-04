

object TraitFilter {
  def unit: String = "Units URI (normalized)"
  def measurementAndUnit = List("Measurement URI", unit)
  def value = "Value"
  def valueOptions = "values"
  def minValue = "minValue"
  def maxValue = "maxValue"

  def hasTrait(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    def traitMax(s: Map[String, String]) = normalizeTraitSelector(s, maxValue)
    def traitMin(s: Map[String, String]) = normalizeTraitSelector(s, minValue)
    val normalizedTraitSelector = (traitMax _ andThen traitMin andThen traitOption)(traitSelector)
    val normalizedRecord = normalizeTraitRecord(record)

    val traitMatchers = Seq(
      compatibleMeasurementAndUnit(normalizedTraitSelector, normalizedRecord),
      valueInOptions(normalizedTraitSelector, normalizedRecord),
      valueLessThan(normalizedTraitSelector, normalizedRecord),
      valueGreaterThan(normalizedTraitSelector, normalizedRecord)
    )
    !traitMatchers.contains(false)
  }

  def normalizeTraitSelector(aTrait: Map[String, String], valueSelector: String): Map[String, String] = {
    (aTrait.get(unit), aTrait.get(valueSelector)) match {
      case (Some("http://purl.obolibrary.org/obo/UO_0000009"), Some(aValue)) => {
        aTrait ++ Map(unit -> "http://purl.obolibrary.org/obo/UO_0000021"
          , valueSelector -> (aValue.toFloat * 1000.0).toString)
      }
      case (Some("http://purl.obolibrary.org/obo/UO_0000021"), Some(aValue)) => {
        aTrait ++ Map(valueSelector -> aValue.toFloat.toString)
      }
      case _ => aTrait
    }
  }

  def traitOption(aTrait: Map[String, String]): Map[String, String] = {
    (aTrait.get(unit), aTrait.get(valueOptions)) match {
      case (Some("http://purl.obolibrary.org/obo/UO_0000009"), Some(aValue)) => {
        val normalizedOptions = splitOptions(aValue).map(_.toFloat * 1000.0).mkString("|")
        aTrait ++ Map(unit -> "http://purl.obolibrary.org/obo/UO_0000021"
          , valueOptions -> normalizedOptions)
      }
      case (Some("http://purl.obolibrary.org/obo/UO_0000021"), Some(aValue)) => {
        val normalizedOptions = splitOptions(aValue).map(_.toFloat).mkString("|")
        aTrait ++ Map(valueOptions -> normalizedOptions)
      }
      case _ => aTrait
    }
  }

  def normalizeTraitRecord(aTrait: Map[String, String]): Map[String, String] = {
    (aTrait.get(unit), aTrait.get(value)) match {
      case (Some("http://purl.obolibrary.org/obo/UO_0000009"), Some(aValue)) => {
        aTrait ++ Map(unit -> "http://purl.obolibrary.org/obo/UO_0000021", value -> (aValue.toFloat * 1000.0).toString)
      }
      case (Some("http://purl.obolibrary.org/obo/UO_0000021"), Some(aValue)) => {
        aTrait ++ Map(value -> aValue.toFloat.toString)
      }
      case _ => aTrait
    }
  }

  def valueGreaterThan(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(maxValue), record.get(value)) match {
      case (Some(maxValueSelector), Some(aValue)) => maxValueSelector > aValue
      case (Some(maxValueSelector), None) => false
      case _ => true
    }
  }

  def valueLessThan(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(minValue), record.get(value)) match {
      case (Some(minValueSelector), Some(aValue)) => minValueSelector < aValue
      case (Some(minValueSelector), None) => false
      case _ => true
    }
  }

  def valueInOptions(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(valueOptions), record.get(value)) match {
      case (Some(valueSelector), Some(aValue)) => {
        val options = splitOptions(valueSelector)
        traitSelector.get(unit) match {
          case (Some("http://purl.obolibrary.org/obo/UO_0000021")) =>
            options.map(_.toFloat).contains(aValue.toFloat)
          case _ =>
            options.contains(aValue)
        }

      }
      case (Some(valueSelector), None) => false
      case _ => true
    }
  }

  def splitOptions(valueSelector: String): Array[String] = {
    valueSelector.split( """\|""")
  }

  def compatibleMeasurementAndUnit(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    measurementAndUnit
      .flatMap(record get)
      .equals(measurementAndUnit.flatMap(traitSelector get))
  }
}
