import org.scalatest._

class ChecklistGenerator$Test extends FlatSpec with Matchers {

  "calling tool" should "print something" in {
    val args: Array[String] = Array("-c", "archive1|archive2|archive3", "-t", "traitArchive1", "-f", "some output format")
    ChecklistGenerator.config(args ++ Array("taxonA|taxonB", "some wkt string", "trait1|trait2")) match {
      case Some(config) => {
        config.occurrenceFiles.size should be(3)
        config.occurrenceFiles should contain("archive1")
        config.traitFiles should contain("traitArchive1")
        config.outputFormat should be("some output format")
        config.geoSpatialSelector should be("some wkt string")
        config.taxonSelector should be(Seq("taxonA", "taxonB"))
        config.traitSelector should be(Seq("trait1", "trait2"))
        config.observedBefore should be(None)
        config.observedAfter should be(None)
        config.sourceSelector should be(Seq())
      }
      case None => fail("should return a valid config object")
    }
  }

  "calling tool" should "print something also" in {
    ChecklistGenerator.config(Array()) match {
      case Some(config) => {
        fail("should return a invalid config object")
      }
      case None => {

      }
    }
  }

}