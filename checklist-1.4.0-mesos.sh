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
--class ChecklistGenerator \
file:///home/int/jobs/iDigBio-LD-assembly-1.4.0.jar \
-f "cassandra" \
-c file:///home/int/data/idigbio/occurrence.txt.parquet \
-t file:///home/int/data/traitbank/bodymass.csv \
"\"Aves|Actinopterygii\"" \
"\"ENVELOPE(-99.14062,-80.15625,30.86451022625836,23.765236889758672)\"" \
"\"bodyMass in 2700|3600 g\"" 
