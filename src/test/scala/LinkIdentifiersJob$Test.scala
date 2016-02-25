import org.scalatest._

class LinkIdentifiersJob$Test extends FlatSpec with Matchers {

  "calling tool with valid arguments" should "create a config object" in {
    LinkIdentifiersJob.config(Array("-o", "file:///myoutputfile", "archive1", "archive2", "archive3")) match {
      case Some(config) => {
        config.archives.size should be(3)
        config.archives should contain("archive1")
        config.outputFile should be("file:///myoutputfile")
        config.outputFormat should be("parquet")
      }
      case None => fail("should return a valid config object")
    }
  }

  "calling tool with required and optional arguments" should "create a config object" in {
    LinkIdentifiersJob.config(Array("-f", "some format", "-o", "file:///myoutputfile", "archive1", "archive2", "archive3")) match {
      case Some(config) => {
        config.archives.size should be(3)
        config.archives should contain("archive1")
        config.outputFile should be("file:///myoutputfile")
        config.outputFormat should be("some format")
      }
      case None => fail("should return a valid config object")
    }
  }

  "calling tool with nothing" should "create a problem" in {
    LinkIdentifiersJob.config(Array()) match {
      case Some(config) => {
        fail("should return a invalid config object")
      }
      case None => {

      }
    }
  }

  "calling tool without outputfile" should "throw a hissy fit" in {
    LinkIdentifiersJob.config(Array("archive1")) match {
      case Some(config) => {
        fail("should return a invalid config object")
      }
      case None => {

      }
    }
  }

}