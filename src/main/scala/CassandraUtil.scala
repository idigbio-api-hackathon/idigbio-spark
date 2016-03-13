import com.datastax.spark.connector.SomeColumns

object CassandraUtil {
  def checklistKeySpaceCreate: String = {
    s"CREATE KEYSPACE IF NOT EXISTS effechecka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }"
  }

  def checklistTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.checklist (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, recordcount int, PRIMARY KEY((taxonselector, wktstring, traitselector), recordcount, taxon))"
  }

  def checklistRegistryTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.checklist_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))"
  }

  def checklistColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "taxon", "recordcount")
  }

  def checklistRegistryColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "status", "recordcount")
  }

  def occurrenceCollectionTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, lat DOUBLE, lng DOUBLE, event_date TIMESTAMP, record_url TEXT, first_added_date TIMESTAMP, archive_url TEXT, PRIMARY KEY((taxonselector, wktstring, traitselector), first_added_date, archive_url, record_url, taxon, event_date, lat, lng))"
  }

  def occurrenceCollectionRegistryTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.occurrence_collection_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))"
  }

  def occurrenceCollectionColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "taxon", "lat", "lng", "event_date", "record_url", "first_added_date", "archive_url")
  }

  def occurrenceCollectionRegistryColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "status", "recordcount")
  }

}
