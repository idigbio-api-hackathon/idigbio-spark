import org.scalatest._

import scala.util.Success

class TraitFilterConfigParser$Test extends FlatSpec with Matchers {

  "parser" should "chop a string" in {
    val parse: TraitFilterConfigParser.ParseResult[String] = TraitFilterConfigParser.parse(TraitFilterConfigParser.term, "bla123 foo bar")
    parse.get shouldBe "bla123"
  }

  "crap input" should "produce an empty trait filter config" in {
    TraitFilterConfigParser.parse(TraitFilterConfigParser.config, "zzzz boom!") match {
      case TraitFilterConfigParser.Success(matched,_) => fail("crappy trait selector should fail")
      case TraitFilterConfigParser.Failure(matched,_) =>
      case TraitFilterConfigParser.Error(matched,_) =>
    }
  }

  "parser" should "produce a trait filter config" in {
    val parse = TraitFilterConfigParser.parse(TraitFilterConfigParser.config, "bodyMass equals 123 kg")
    val expected = Map(
      """Measurement URI""" -> """http://purl.obolibrary.org/obo/VT_0001259""",
      """values""" -> """123""",
      """Units URI (normalized)""" -> """http://purl.obolibrary.org/obo/UO_0000009"""
    )
    parse.get shouldBe expected
  }

  "parser" should "produce a trait filter config with list" in {
    val parse = TraitFilterConfigParser.parse(TraitFilterConfigParser.config, "bodyMass in 123|444 kg")
    val expected = Map(
      """Measurement URI""" -> """http://purl.obolibrary.org/obo/VT_0001259""",
      """values""" -> """123|444""",
      """Units URI (normalized)""" -> """http://purl.obolibrary.org/obo/UO_0000009"""
    )
    parse.get shouldBe expected
  }

  "parser" should "produce a trait filter config without unit" in {
    val parse = TraitFilterConfigParser.parse(TraitFilterConfigParser.config, "bodyMass equals 123")
    val expected = Map(
      """Measurement URI""" -> """http://purl.obolibrary.org/obo/VT_0001259""",
      """values""" -> """123"""
    )

    parse.get shouldBe expected
  }


}
