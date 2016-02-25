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
--class DarwinCoreToParquet \
file:///home/int/jobs/iDigBio-LD-assembly-1.2.jar \
file:///home/int/data/gbif/meta.xml \
