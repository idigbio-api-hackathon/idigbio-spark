import org.scalatest._
import org.apache.spark.sql._

class IdentifierUtil$Test extends TestSparkContext {
  
  "a row" should "be transformed into a list of links" in {
    
    val aRow = Row.fromTuple ("one", "two", "three")
    
     
    
  }

  "identifier as" should "al" in {
    
    val occ = sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").
      option("inferSchema", "true").
      load(getClass().getResource("idigbio/occurrence.txt").toExternalForm())
    occ.count() should be(9)    
    
    val externalIdColumns = occ.schema.
        filter(_.dataType == org.apache.spark.sql.types.StringType).
         map(_.name.trim).
         filter(IdentifierUtil.idigBioColumns.contains(_))         
         
    val idsOnly = occ.select(IdentifierUtil.idigbioId, externalIdColumns: _*)
    for ((startIdCol, endIdCol) <- IdentifierUtil.mapTuples("id", externalIdColumns))  {      
      val link_map = idsOnly.drop(endIdCol)
        .select(idsOnly(startIdCol), idsOnly(endIdCol))
        .withColumnRenamed(startIdCol, "start_id")
        .withColumnRenamed(endIdCol, "end_id")
        .withColumn("link_rel", idsOnly("link_rel"))
        .select(idsOnly("start_id"), idsOnly("link_rel"), idsOnly("start_id"))
        
       link_map
        .take(10)
        .foreach(println(_)) 
    } 
  }

}
