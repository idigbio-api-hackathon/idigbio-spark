# Submit a spark job against a Mesos-Spark configuration.
# see http://spark.apache.org/docs/latest/running-on-mesos.html
#
# SPARK_DATA: where datasets are 
# SPARK_JOBS: where spark job jars sit
# SPARK_HOME: where apache has been installed
#


DATASET_DIR=file:///home/int/data

$SPARK_HOME/bin/spark-submit \
--master mesos://apihack-c18.idigbio.org:7077 \
--deploy-mode cluster \
--executor-memory 6G \
--driver-memory 6G \
--total-executor-cores 5 \
--class RankIdentifiersJob \
file:///home/int/jobs/iDigBio-LD-assembly-1.3.5.jar \
-f com.databricks.spark.csv \
-i parquet \
-o $DATASET_DIR/rank_ids_manual_idigbio_gbif.txt \
$DATASET_DIR/idigbio/id_links.txt.parquet \
$DATASET_DIR/gbif/id_links.txt.parquet \
$DATASET_DIR/phoibos2/id_links.csv.parquet 
