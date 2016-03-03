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
--executor-memory 20G \
--driver-memory 6G \
--total-executor-cores 10 \
--class RankIdentifiersJob \
file:///home/int/jobs/iDigBio-LD-assembly-1.3.5.jar \
-f com.databricks.spark.csv \
-i parquet \
-o $DATASET_DIR/rank_ids_idigbio_gbif.csv \
$DATASET_DIR/idigbio/occurrence.txt.parquet \
$DATASET_DIR/gbif/occurrence.txt.parquet
