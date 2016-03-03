# Submit a spark job against a Mesos-Spark configuration.
# see http://spark.apache.org/docs/latest/running-on-mesos.html
#
# SPARK_DATA: where datasets are 
# SPARK_JOBS: where spark job jars sit
# SPARK_HOME: where apache has been installed
#


$SPARK_HOME/bin/spark-submit \
--master mesos://apihack-c18.idigbio.org:7077 \
--deploy-mode cluster \
--executor-memory 20G \
--driver-memory 6G \
--total-executor-cores 10 \
--class LinkIdentifiersJob \
file:///home/int/jobs/iDigBio-LD-assembly-1.3.5.jar \
-o file:///home/int/data/id_links_gbif_idigbio.txt.parquet \
file:///home/int/data/gbif/occurrence.txt.parquet \
file:///home/int/data/idigbio/occurrence.txt.parquet 
