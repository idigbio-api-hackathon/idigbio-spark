import scala.xml.XML
import scala.io.Source
import java.net.URL
import java.io.File

object DwC {

  case class Meta(coreTerms: Seq[String], delimiter: String, fileLocations: Seq[String], skipHeaderLines: Int)

  def readMeta(metaURL: URL): Option[Meta] = try {
    val meta = XML.load(metaURL)
    val delimiter = (meta \\ "core" \\ "@fieldsTerminatedBy") map { _ text } headOption match {
      case Some(d) => d
      case None    => ","
    }
    val skipHeaderLines = (meta \\ "core" \\ "@ignoreHeaderLines") map { _ text } headOption match {
      case Some(d) => Integer.parseInt(d)
      case None    => 0
    }
    val fieldTerms = (meta \\ "core" \\ "@term") map { _ text }
    val baseURLParts = metaURL.toString.split("/").reverse.tail.reverse
    val locations = (meta \\ "core" \\ "location") map {
      location => (baseURLParts ++ List(location text)) mkString ("/")
    }
    Some(Meta(fieldTerms, delimiter, locations, skipHeaderLines))
  } catch {
    case e: Exception =>
      println(s"failed to read from [$metaURL]")
      None
  }
}
