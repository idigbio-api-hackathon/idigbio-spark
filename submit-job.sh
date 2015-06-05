# SPARK_DATA: where datasets are 
# SPARK_JOBS: where spark job jars sit

$SPARK_HOME/bin/spark-submit --class RecordLinker $SPARK_JOBS/iDigBio-LD-assembly-1.0.jar $SPARK_DATA/idigbio/occurrence.txt $1
