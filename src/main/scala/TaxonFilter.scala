

object TaxonFilter {

  def taxonFields = List( "dwc:kingdom", "dwc:phylum", "dwc:order", "dwc:class", "dwc:family", "dwc:genus", "dwc:scientificName")


  def hasTaxa(taxa: List[String], record: Map[String, String]): Boolean = {
    taxonFields
      .flatMap(record get)
      .filter(taxa.contains(_)).nonEmpty
  }

}
