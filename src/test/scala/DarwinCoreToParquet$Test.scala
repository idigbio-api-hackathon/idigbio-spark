import org.scalatest._
import scala.xml.XML
import scala.io.Source
import java.net.URL
import java.io.File

class DarwinCoreToParquet$Test extends FlatSpec with Matchers {

  "calling tool" should "print something" in {
    DarwinCoreToParquet.config(Array("archive1", "archive2", "archive3")) match {
      case Some(config) => {
        config.archives.size should be(3)
        config.archives should contain("archive1")
      }
      case None => fail("should return a valid config object")
    }
  }

  "calling tool" should "print something also" in {
    DarwinCoreToParquet.config(Array()) match {
      case Some(config) => {
        fail("should return a invalid config object")
      }
      case None => {

      }
    }
  }

}

class DwCToParquet$Test extends TestSparkContext with DwCSparkHandler {
  
  "combining metas" should "turn up with aggregated records" in {
    val metas = List("/gbif/meta.xml", "/idigbio/meta.xml") map { getClass().getResource(_) }    
    handle(metas map { _.toString })        
  }

}
