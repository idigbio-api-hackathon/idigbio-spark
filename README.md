# idigbio-spark
processing engine for iDigBio archives

python scripts runner [spark-submit wordcount.py] ran out of heapspace... seems that memory management is a little more solid when using scala.

# how to build Spark Job
```
sbt assembly
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
