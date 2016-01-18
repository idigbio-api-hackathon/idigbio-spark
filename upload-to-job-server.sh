# SPARK_DATA: where datasets are 
# SPARK_JOBS: where spark job jars sit
# SPARK_HOME: where apache has been installed

JAR=target/scala-2.10/iDigBio-LD-assembly-1.1.jar
JOBSERVER_HOST=localhost
curl --verbose --data-binary @$JAR $JOBSERVER_HOST:8090/jars/idigbio
