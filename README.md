## Run the application
The following code launch the application in yarn client mode :

    spark-submit --master yarn --deploy-mode client --class msc.Streamer --conf spark.hadoop.mapred.output.compress=false twitter-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar en 100 15 cloud data

where:
 - "en" is the language upon which the tweets are filtered,
 - "100" is the number of tweets to retrieved (at least). When that number is reached, the pipeline is stopped which means that those already in the pipeline will also be saved,
 - "15" : spark streaming context batch duration (in second)
 - "cloud" "data" .. : list of keywords upon which tweets will be matched

In order to run, this application requires the property **yarn.scheduler.maximum-allocation-vcores >= 2**. This can set from Ambari or directly in /etc/hadoop/conf/yarn-site.xml.
 
The tweets RDDs are saved in directories /user/alain-T/tweets/time=*timestamp* where *timestamp* is the RDD time as provided in foreachRDD

# Populate a Hive table from text files in HDFS

The following command creates an external table in Hive referencing the files saved by the application :

    hive -f hive.sql

Note: that Hive script creates a database named 'tweet_db' containing a a table named 'tweets' and it expects them not to exist before being run.

It is not the case, the following Hive command can be used to erase them :

    -- remove tweets table
    hive> use tweet_db;
    hive> drop table tweets;
    
    -- remove tweet_db database
    hive> use default;
    hive> drop database tweet_db;

## How to count the tweets in the dataset ?

The following commands displays the number of tweets stored in the table:

    $ hive 
    hive> use tweet_db; 
    hive> select count(*) from tweets;
    ...
    OK
    120
    
Note: the number returned matches the number of lines of the files saved by the application that can be computed using the following command:

 `hdfs dfs -cat /user/alain.tholon/tweets/time=*/p* | wc`

## Process the dataset
The following command processes the dataset :

    $ spark-shell --master yarn --deploy-mode client
    scala> :load spark-shell-data-processing.scala
