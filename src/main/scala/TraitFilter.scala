

object TraitFilter {
  def measurementAndUnit = List("Measurement URI", "Units URI (normalized)")

  def value = "Value"

  def valueOptions = "values"

  def minValue = "minValue"

  def maxValue = "maxValue"

  def hasTrait(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    !Seq(
      sameMeasurementAndUnit(traitSelector, record),
      valueInOptions(traitSelector, record),
      valueLessThan(traitSelector, record),
      valueGreaterThan(traitSelector, record)
    ).contains(false)
  }

  def valueGreaterThan(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(maxValue), record.get(value)) match {
      case (Some(maxValueSelector), Some(aValue)) => maxValueSelector.toFloat > aValue.toFloat
      case (Some(maxValueSelector), None) => false
      case _ => true
    }
  }

  def valueLessThan(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(minValue), record.get(value)) match {
      case (Some(minValueSelector), Some(aValue)) => minValueSelector.toFloat < aValue.toFloat
      case (Some(minValueSelector), None) => false
      case _ => true
    }
  }

  def valueInOptions(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    (traitSelector.get(valueOptions), record.get(value)) match {
      case (Some(valueSelector), Some(aValue)) => {
        valueSelector.split( """\|""").contains(aValue)
      }
      case (Some(valueSelector), None) => false
      case _ => true
    }
  }

  def sameMeasurementAndUnit(traitSelector: Map[String, String], record: Map[String, String]): Boolean = {
    measurementAndUnit
      .flatMap(record get)
      .equals(measurementAndUnit.flatMap(traitSelector get))
  }
}
