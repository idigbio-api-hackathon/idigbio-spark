import me.prettyprint.hector.api.factory.HFactory
import org.cassandraunit.DataLoader
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet
import org.scalatest._

class CassandraWriter$Test extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    //EmbeddedCassandraServerHelper.startEmbeddedCassandra()
  }

  "saving a row" should "persist in cassandra" in {
    val clusterName = "TestCluster"
    val hostPort = "localhost:9160"
    val dataLoader = new DataLoader(clusterName, hostPort)
    dataLoader.load(new ClassPathYamlDataSet("sampleDataSet.yaml"))
    val cluster = HFactory.getOrCreateCluster(clusterName, hostPort);
    val keyspace = HFactory.createKeyspace("beautifulKeyspaceName", cluster);
    keyspace should not be null


  }


}
