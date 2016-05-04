import com.datastax.spark.connector.{ColumnRef, SomeColumns}

object CassandraUtil {
  val selectorColumns: Seq[String] = Seq("taxonselector", "wktstring", "traitselector", "sourceselector", "observedbefore", "observedafter")
  val selectorSchema = selectorColumns.map(_ + " TEXT").mkString(",")
  val selectorColumnSelect: String = selectorColumns.mkString(",")

  def checklistKeySpaceCreate: String = {
    s"CREATE KEYSPACE IF NOT EXISTS effechecka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }"
  }

  def checklistTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.checklist ($selectorSchema, taxon TEXT, recordcount int, PRIMARY KEY(($selectorColumnSelect), recordcount, taxon))"
  }


  def checklistRegistryTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.checklist_registry ($selectorSchema, status TEXT, recordcount int, PRIMARY KEY($selectorColumnSelect))"
  }

  def checklistColumns: SomeColumns = {
    asSomeColumns(selectorColumns ++ Seq("taxon", "recordcount"))
  }

  def checklistRegistryColumns: SomeColumns = {
    asSomeColumns(selectorColumns ++ Seq("status", "recordcount"))
  }

  def occurrenceCollectionTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection ($selectorSchema, taxon TEXT, lat DOUBLE, lng DOUBLE, start TIMESTAMP, end TIMESTAMP, id TEXT, added TIMESTAMP, source TEXT, PRIMARY KEY((taxonselector, wktstring, traitselector), added, source, id, taxon, start, end, lat, lng))"
  }

  def occurrenceCollectionRegistryTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection_registry ($selectorSchema, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))"
  }


  def occurrenceCollectionColumns: SomeColumns = {
    asSomeColumns(selectorColumns ++ Seq("taxon", "lat", "lng", "start", "end", "id", "added", "source"))
  }

  def asSomeColumns(columnNames: Seq[String]): SomeColumns = {
    SomeColumns(columnNames.map(x => x: ColumnRef): _*)
  }

  def occurrenceCollectionRegistryColumns: SomeColumns = {
    asSomeColumns(selectorColumns ++ Seq( "status", "recordcount"))
  }

}
