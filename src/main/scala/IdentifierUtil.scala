import com.datastax.spark.connector.SomeColumns

object IdentifierUtil {

  def idigbioId = "id"

  def idigBioColumns: List[String] = {
    List("dcterms:bibliographicCitation",
      "dwc:identificationReferences",
      "dwc:ownerInstitutionCode",
      "dwc:collectionCode",
      "dwc:occurrenceID",
      "dwc:associatedMedia",
      "dwc:catalogNumber",
      "dwc:identificationReferences",
      "dwc:associatedSequences",
      "dwc:associatedOccurrences",
      "dwc:scientificNameID",
      "dwc:namePublishedIn",
      "dwc:relatedResourceID")
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

  def gbifId = "gbifID"

  def gbifColumns: List[String] = {
    List("gbifID",
      "bibliographicCitations",
      "references",
      "associatedOccurrences",
      "associatedReferences",
      "associatedSequences",
      "associatedTaxa",
      "collectionID",
      "datasetID",
      "identificationID",
      "institutionCode",
      "institutionID",
      "occurrenceID",
      "recordNumber",
      "scientificNameID")
  }

  def read() = {
    //val gbif = sqlContext.read.format("parquet").load("/home/int/data/gbif/occurrence")
    //val idigbio = sqlContext.read.load("/home/int/data/idigbio/occurrence.parquet")
  }

}
