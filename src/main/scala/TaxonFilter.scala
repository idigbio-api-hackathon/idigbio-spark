

object TaxonFilter {

  def hasTaxa(taxa: List[String], record: Map[String, String]): Boolean = {
    List("dwc:scientificName", "dwc:kingdom", "dwc:phylum", "dwc:order", "dwc:class", "dwc:family", "dwc:genus")
      .flatMap(record get)
      .filter(taxa.contains(_)).nonEmpty
  }

}
