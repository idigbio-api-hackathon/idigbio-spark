import org.joda.time.chrono.ISOChronology
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone, Interval}

object DateUtil {

  def validDate(dateString: String): Boolean = {
    null != dateString && dateString.nonEmpty &&
      (try {
        new DateTime(dateString, DateTimeZone.UTC)
        true
      } catch {
        case e: IllegalArgumentException =>
          try {
            null != Interval.parse(dateString)
          } catch {
            case e: IllegalArgumentException => false
          }
      })
  }

  def startDate(dateString: String): Long = {
    if (dateString.contains("/")) {
      toInterval(dateString).getStartMillis
    } else {
      toUnixTime(dateString)
    }
  }

  def toInterval(dateString: String): Interval = {
    new Interval(dateString, ISOChronology.getInstance(DateTimeZone.UTC))
  }

  def endDate(dateString: String): Long = {
    if (dateString.contains("/")) {
      toInterval(dateString).getEndMillis
    } else {
      toUnixTime(dateString)
    }
  }

  def toUnixTime(dateString: String): Long = {
    new DateTime(dateString, DateTimeZone.UTC).toDate.getTime
  }

  def basicDateToUnixTime(basicDateString: String): Long = {
    val fmt = ISODateTimeFormat.basicDate()
    fmt.withZoneUTC().parseDateTime(basicDateString).toDate.getTime
  }
}
