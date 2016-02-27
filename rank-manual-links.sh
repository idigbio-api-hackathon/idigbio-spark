# Submit a spark job against a Mesos-Spark configuration.
# see http://spark.apache.org/docs/latest/running-on-mesos.html
#
# SPARK_DATA: where datasets are 
# SPARK_JOBS: where spark job jars sit
# SPARK_HOME: where apache has been installed
#


DATASET_DIR=file:///home/int/data/phoibos2
DATASET_OUT=file:///home/int/data/tmp

$SPARK_HOME/bin/spark-submit \
--master mesos://apihack-c18.idigbio.org:7077 \
--deploy-mode cluster \
--executor-memory 6G \
--driver-memory 6G \
--total-executor-cores 5 \
--class RankIdentifiersJob \
file:///home/int/jobs/iDigBio-LD-assembly-1.3.5.jar \
-f com.databricks.spark.csv \
-o $DATASET_DIR/rank_ids.txt \
-i com.databricks.spark.csv \
$DATASET_DIR/**/*id_links.csv

