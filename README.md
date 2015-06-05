# idigbio-spark
Links iDigBio records to GenBank, Global Names and other projects.

[![Build Status](https://travis-ci.org/idigbio-api-hackathon/idigbio-spark.svg?branch=master)](https://travis-ci.org/idigbio-api-hackathon/idigbio-spark)

# how to build Spark Job
```sh
sbt assembly
# link w/ genbank
./submit-job genbank
# link w/ global names
./submit-job globalnames
```

# running the spark job
see submit.sh script.

Sample ID link table:

idigbio record id | external id | 
 --- | ---
 44e8-813e-3c68380f3e01 | GBIF:123
 44e8-813e-3c68380f3e01 | MYCOBANK:123
 44e8-813e-3c68380f3e01 | GNI:abd2343a123

# Example for MycoBank integation

name | MycoId
 --- | ---
 Tulostoma | 19339
 ... | ...
