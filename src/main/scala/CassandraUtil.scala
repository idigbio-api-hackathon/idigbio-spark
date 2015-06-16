import com.datastax.spark.connector.SomeColumns

object CassandraUtil {

  def checklistTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS idigbio.checklist (taxonselector TEXT, wktstring TEXT, taxon TEXT, recordcount int, PRIMARY KEY((taxonselector, wktstring), recordcount, taxon))"
  }

  def checklistRegistryTableCreate: String = {
    s"CREATE TABLE IF NOT EXISTS idigbio.checklist_registry (taxonselector TEXT, wktstring TEXT, status TEXT, recordcount int, PRIMARY KEY(taxonselector, wktstring))"
  }

  def checklistKeySpaceCreate: String = {
    s"CREATE KEYSPACE IF NOT EXISTS idigbio WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }"
  }

  def checklistColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "taxon", "recordcount")
  }

  def checklistRegistryColumns: SomeColumns = {
    SomeColumns("taxonselector", "wktstring", "status", "recordcount")
  }

}
