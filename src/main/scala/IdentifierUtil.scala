import org.apache.spark.graphx.Edge
import org.apache.spark.sql.Row

import scala.util.hashing.MurmurHash3

object IdentifierUtil {

  def idigbioColumns: List[String] = {
    "id" :: dwcIdColumns
  }

  def gbifColumns: List[String] = {
    "http://rs.gbif.org/terms/1.0/gbifID" :: dwcIdColumns
  }

  def dwcIdColumns: List[String] = {
    List("http://purl.org/dc/terms/bibliographicCitation",
      "http://rs.tdwg.org/dwc/terms/identificationReferences",
      "http://rs.tdwg.org/dwc/terms/ownerInstitutionCode",
      "http://rs.tdwg.org/dwc/terms/collectionCode",
      "http://rs.tdwg.org/dwc/terms/occurrenceID",
      "http://rs.tdwg.org/dwc/terms/associatedMedia",
      "http://rs.tdwg.org/dwc/terms/catalogNumber",
      "http://rs.tdwg.org/dwc/terms/identificationReferences",
      "http://rs.tdwg.org/dwc/terms/associatedSequences",
      "http://rs.tdwg.org/dwc/terms/associatedOccurrences",
      "http://rs.tdwg.org/dwc/terms/scientificNameID",
      "http://rs.tdwg.org/dwc/terms/namePublishedIn",
      "http://rs.tdwg.org/dwc/terms/relatedResourceID")
  }

  def toEdge(row: Row) = {
    if (row.isNullAt(0) || row.isNullAt(1) || row.isNullAt(2)) {
      None
    } else {
      Some(Edge(MurmurHash3.stringHash(row.getString(0)),
        MurmurHash3.stringHash(row.getString(2)),
        row.getString(1)))
    }
  }

  def toVertices(row: Row) = {
    if (row.isNullAt(0) || row.isNullAt(2)) {
      Seq()
    } else {
      val src = row.getString(0)
      val dst = row.getString(2)
      Seq((MurmurHash3.stringHash(src).toLong, src),
        (MurmurHash3.stringHash(dst).toLong, dst))
    }
  }


}
