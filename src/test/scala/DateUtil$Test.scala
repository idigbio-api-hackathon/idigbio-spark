import org.scalatest._
import DateUtil._

class DateUtil$Test extends FlatSpec with Matchers {

  "a date with year and month" should "be valid" in {
    validDate("1965-1") should be(true)
  }

  "a date with year and month no hyphen" should "be valid" in {
    validDate("19650101") should be(true)
  }

  "a date interval with year and month" should "be valid" in {
    validDate("1965-1/1970") should be(true)
  }

  "a start date with year and month" should "parse using UTC" in {
    startDate("1965-1") should be(-157766400000L)
  }

  "an end date with year and month" should "parse using UTC" in {
    endDate("1965-1") should be(-157766400000L)
  }

  "start of date interval with year and month" should "parse using UTC" in {
    startDate("1965-1/1970") should be(-157766400000L)
  }

  "end of date interval with year and month" should "parse using UTC" in {
    endDate("1965-1/1970") should be(0L)
  }

  "valid date" should "be valid" in {
    DateUtil.validDate("2016-01-1") should be(true)
  }

  "valid date range" should "be valid" in {
    DateUtil.validDate("2016-01-10/2016-01-20") should be(true)
  }

  "invalid date" should "be not valid" in {
    DateUtil.validDate("boo 2016-01-1") should be(false)
  }


}
