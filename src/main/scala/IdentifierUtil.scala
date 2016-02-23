import com.datastax.spark.connector.SomeColumns

object IdentifierUtil {

  def idigbioColumns: List[String] = {
    List("id") ++ dwcIdColumns
  }

  def gbifColumns: List[String] = {
    List("http://rs.gbif.org/terms/1.0/gbifID") ++ dwcIdColumns
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


}
