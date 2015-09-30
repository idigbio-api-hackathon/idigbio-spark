import com.datastax.spark.connector.SomeColumns

object CassandraUtil {

  def checklistTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.checklist (taxonselector TEXT, wktstring TEXT, traitselector TEXT, taxon TEXT, recordcount int, PRIMARY KEY((taxonselector, wktstring, traitselector), recordcount, taxon))"
  }

  def checklistRegistryTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS effechecka.checklist_registry (taxonselector TEXT, wktstring TEXT, traitselector TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring, traitselector))"
  }

  def checklistKeySpaceCreate: String = {
    s"CREATE KEYSPACE IF NOT EXISTS effechecka WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }"
  }

  def checklistColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "taxon", "recordcount")
  }

  def checklistRegistryColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "traitselector", "status", "recordcount")
  }

}
