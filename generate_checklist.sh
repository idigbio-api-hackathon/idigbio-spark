# SPARK_DATA: where datasets are 
# SPARK_JOBS: where spark job jars sit
# SPARK_HOME: where apache has been installed
#
# example:
# sh generate_checklist.sh "Insecta|Mammalia" "ENVELOPE(-150,-50,40,10)"

$SPARK_HOME/bin/spark-submit --class ChecklistGenerator $SPARK_JOBS/iDigBio-LD-assembly-1.0.jar $SPARK_DATA/idigbio-100k/occurrence.txt $@
