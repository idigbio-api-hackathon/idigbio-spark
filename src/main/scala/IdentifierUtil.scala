import com.datastax.spark.connector.SomeColumns

object IdentifierUtil {

  def idigbioId = "id"

  def idigBioColumns: List[String] = {
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

  def mapTuples(id: String, columnNames: Seq[String]): Seq[(String, String)] = {
    columnNames.map((id, _))
  }

  def idigBioTuples: Seq[(String, String)] = {
    mapTuples("id", idigBioColumns)
  }

  def gbifTuples = {
    mapTuples("gbifID", gbifColumns)
  }

  def gbifId = "`http://rs.gbif.org/terms/1.0/gbifID`"

  def gbifColumns: List[String] = {
    idigBioColumns
  }

  def read() = {
    //val gbif = sqlContext.read.format("parquet").load("/home/int/data/gbif/occurrence")
    //val idigbio = sqlContext.read.load("/home/int/data/idigbio/occurrence.parquet")
  }

}
