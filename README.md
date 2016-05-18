**This repository has been forked and obsoleted by [https://github.com/bio-guoda/idigbio-spark]<https://github.com/bio-guoda/idigbio-spark>. Please continue development there.**

# idigbio-spark
Links iDigBio records to [GenBank](http://www.ncbi.nlm.nih.gov/genbank/), [Global Names](http://globalnames.org) and possibly other open bioinformatics resources. The purpose of this project is to **link specimen records to external systems like GenBank and Global Names at scale** by using [Apache's Spark](http://spark.apache.org) a distributed data processing tool. 

[![Build Status](https://travis-ci.org/idigbio-api-hackathon/idigbio-spark.svg?branch=master)](https://travis-ci.org/idigbio-api-hackathon/idigbio-spark)

# prerequisites
 * Apache Spark v1.3.1 installed at ```$SPARK_HOME```
 * iDigBio Archive - Download iDigBio DarwinCore Archive unpack ```$SPARK_DATA```
 * Java SDK 1.7+ 
 * Scala Build Tool (Sbt) v0.13.8
 * git 

# install
Clone this repository using ```git clone git@github.com:idigbio-api-hackathon/idigbio-spark.git``` 

# how to build Spark Job
```sh
cd [idigbio-spark directory]
# build the idigbio-spark job jar
sbt assembly
# link w/ genbank
./submit-job genbank
# link w/ global names
./submit-job globalnames
```

# expected result
```
$ ls $SPARK_HOME 
CHANGES.txt README.md conf    examples  sbin
LICENSE   RELEASE   data    lib
NOTICE    bin   ec2   python
$ ls $SPARK_DATA/idigbio
meta.xml
multimedia.txt
occurrence.txt
$ sh submit-job.sh genbank
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
15/06/06 10:51:26 INFO SparkContext: Running Spark version 1.3.1
15/06/06 10:51:41 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
...
$ cd $SPARK_DATA/idigbio/occurrence.txt.links
$ ls | head
_SUCCESS
part-00000
part-00001
part-00002
part-00003
part-00004
part-00005
part-00006
part-00007
part-00008
```

# output
the resulting link table looks something like:

```
00f8efa0-75ee-45c1-a88d-8a853705c6dd,gn:813583ad-c364-5c15-b01a-43eaa1446fee
004ba454-7cce-4f6d-8a22-bf4aa34981df,gn:dc74c890-d31c-5fb4-aaad-466bf14bb215
00350756-ba40-4fda-8db3-ade4b470160c,gn:e3992c38-df0c-5be6-839c-d84c8edabfd1
```
where the first column is the idigbio record UUID and the second column is the global name hash associated with the taxonomic name of the specimen.

# performance results 
two systems where used: 

**Macbook** 
Model 13-inch, Aluminum, Late 2008
Processor  2.4 GHz Intel Core 2 Duo
Memory  8 GB 1067 MHz DDR3
Software  OS X 10.9.5 (13F1077)

**an iDigBio Server**
Model ?
Processor  32 Core (?)
Memory  128 GB (?)
Software  Ubuntu

 #records | mac duration | server duration 
 --- | --- | ---
 100k | 10s | 4s 
 1M | 83s | 11s
 15M | 21m | 2m

For all runs CPU utilization was at ~80% for both mac and server systems. This means that the spark job kept 2 CPUs busy on the mac system and about 32 CPUs busy on the server system.

# conclusion
The spark job created to process large amounts of iDigBio was able to scale from a laptop to a powerful 32 core server *without any modifications* to the job. This shows the benefits of Apache's Spark - after "sparkizing" the processing algorithms, large amounts of data can be processed on small (laptop) and large (server) servers, where spark figures out how to optimially use the available resources.

Spark jobs can be implemented in Scala, Java and Python. An early attempt to implement the job in Python caused the process to crash due to an out of memory issue. A later implementation in Scala was stable on both small (macbook) and large (32 core server) system. This seems to suggest that Python support might require some more configuration tweaking to get the jobs to complete reliably, while the Scala implementation work without any configuration changes. 

These preliminary results seem to suggest that Apache Spark is suitable for stable and performant processing of large (~GB) datasets like iDigBio. Our implementation algorithms that link iDigBio to Global Names and GenBank will hopefully help make it easier to discover iDigBio specimen records and their associations with other web-connected resources. 
