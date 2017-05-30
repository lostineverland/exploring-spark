# Exploring Spark

## Exercise

> Calculate User recency, frequency of visits
> We have a table user_visits table partitioned by hour (yyyymmddHH)
> Columns:
> visit_ts timestamp,
> domain_name string,
> page_url string,
> user_id
>  
> I want to build a table of Recency and Frequency of each user visiting a domain. The table should refresh daily once a day. Using this table I need to be able to query for each user domain combo the frequency of the visits in last 30 days, when was the first (lifetime) and the last visit. Implement using Spark2.0 or Hive SQL


## Breaking down the problem

### Input

First a look at what the data looks like.

| visit_ts | domain_name | page_url | user_id |
|--|--|--|--|
| TIMESTAMP | STRING | STRING | STRING |

### Requirements

* build a table of Recency and Frequency of each user visiting a domain
* table should refresh daily once a day
* query for each user domain combo
  - frequency of the visits in last 30 days
  - when was the first visit (lifetime)
  - when was the first last (lifetime)
* implement using Spark2.0 or Hive SQL

### Parsing Requirements

#### 1 Table

My first thoughts on this table are drawn from a look at frequency. From my background in numerical analysis I think of a Fourier or Laplace transforms. However, looking at the requirements, `frequency of the visits in the last 30 days` could be reported simply as a daily average (visits/day). I believe simple is always better than complex; it allows for a quicker turn-around time that presents relevant information. Once it is understood that this is useful, discussions can be had about how precise & accurate the value needs to be.

#### 2 Daily Update

When considering periodic updates the first thing that comes to mind is a cron job. However, depending on the infrastructure, it cold be accomplished in many ways, such as:

* cron
* AWS Lambda recurring function
* if batch jobs are already scheduled in hadoop, this could be just one more `jar` to execute
* ~~presumably, Spark may also have a feature to handle this task~~

Once there's a solution established for  `how to create the table`, this could take shape (though it can also inform/influence the former).

Because this is timeseries data, it is relevant to consider partitioning the data based on a time interval. For this particular exercise a daily partition is relevant, thus it would be worth adding an additional column for day, which could be used to specify the partitioning.

#### 3 Query

The table rows shall be keyed by the combination of `user_id` & `domain_name`.

* Frequency
  - The frequency will be calculated as a daily average over the past 30 days.
* First visit
  - Min timestamp for each key
* Last visit
  - Max timestamp for each key

## Methodology

It's best to try and fail on a tight feedback loop. To accomplish this, setting up a small Spark dev environment would allow for real testing and feedback.

To obtain feedback on queries there must first exist some representative data to operate on.
Once an environment is available the actual querying will be easy with some iterations.

Once the queries are established, updating a table periodically can take shape.

The steps to follow are:

1. Create a dev environment
1. Generate source data
1. Produce the queries which generate the results
1. Create/Update tables

### Dev Env

A quick search for Spark 2.0 on docker revealed an accessible version of [Apache Spark on Docker](https://hub.docker.com/r/aghorbani/spark/).

To setup the container environment:

#### Setup Container
```
> git clone git@github.com:a-ghorbani/docker-spark.git
> cd docker-spark
> docker pull aghorbani/spark:2.1.0
> docker build --rm -t aghorbani/spark:2.1.0 .
```
#### Start Container
```
> docker run \
 -v $HOME/exploring-spark/src:/data \
 -it \
 -p 8088:8088 \
 -p 8042:8042 \
 -p 4040:4040 \
 -h sandbox aghorbani/spark:2.1.0 \
 bash
```

### Test Data

With a working environment for executing jobs/queries, it follows that test data is generated. The following characteristics were considered when generating test data

* collisions in time stamp are possible
* data may not arrive in chronological order
* combine list of domains with list of users (for simplicity `user == user_id`)
* use random integers to generate paths of url
* for this exercise `STRING` timestamps are sufficient as the comparator operators (`>`, erc.) work with ISO-8601 dates

#### Generate Source Data

A simple [python script](./src/generateData.py) was used to generate the data. From brief experience, pyspark appears to have some name space curves which were not resolved here. Nonetheless, the script is still functional for generating the source data.

*On the Container*
```
bash-4.1# pyspark
>>> import sys
>>> sys.path.append('/data')
>>> from generateData import *
>>> pageviews = sc.parallelize(build_tables()).toDF(["timestamp", "domain_name", "page_url", "user"])
>>> pageviews.write.saveAsTable(name='pageviews', format='parquet')
```

Here's a quick look at what was generated:
```
>>> spark.sql('select * from pageviews limit 10').show()
+----------------+-----------------+--------------------+-----------+
|       timestamp|      domain_name|            page_url|       user|
+----------------+-----------------+--------------------+-----------+
|2017-03-31T14:30|     www.espn.com|   www.espn.com/6262|   Benjamin|
|2017-03-31T14:01|     www.espn.com|   www.espn.com/8849|      James|
|2017-03-31T14:49| www.facebook.com|www.facebook.com/...|    Cameron|
|2017-03-31T14:04|    www.yahoo.com|  www.yahoo.com/9342|    Brandon|
|2017-03-31T14:15|    www.yahoo.com|  www.yahoo.com/3549|   Benjamin|
|2017-03-31T14:56|      www.cnn.com|    www.cnn.com/9351|       Ryan|
|2017-03-31T14:28|   www.google.com| www.google.com/7600|     Jordan|
|2017-03-31T14:33|www.pinterest.com|www.pinterest.com...|Christopher|
|2017-03-31T14:21|    www.yahoo.com|  www.yahoo.com/1027|  Christian|
|2017-03-31T14:20|  www.twitter.com|www.twitter.com/4556|       Jack|
+----------------+-----------------+--------------------+-----------+
```

### Produce Results

For the frequency, the daily average over the last 30 days is very small, thus a weekly average was used instead. The remaining values are standard:

```
spark.sql('''
  select count(*) as cnt
    , user
    , count(*) / 30 * 7 as weekly_visits
    , min(timestamp) as first
    , max(timestamp) as last
    , domain_name
  from pageviews
  where timestamp > '2017-04-29'
    -- and domain_name = 'www.espn.com'
  group by domain_name
    , user
  order by user, first
''').show(50)
+---+---------+-------------------+----------------+----------------+-----------------+
|cnt|     user|      weekly_visits|           first|            last|      domain_name|
+---+---------+-------------------+----------------+----------------+-----------------+
|  5|Alexander| 1.1666666666666665|2017-04-29T14:06|2017-05-18T14:39|    www.pixar.com|
|  3|Alexander| 0.7000000000000001|2017-05-01T14:28|2017-05-25T14:53|www.pinterest.com|
|  4|Alexander| 0.9333333333333333|2017-05-01T14:58|2017-05-13T14:35|   www.google.com|
|  3|Alexander| 0.7000000000000001|2017-05-02T14:50|2017-05-29T14:22|    www.yahoo.com|
|  3|Alexander| 0.7000000000000001|2017-05-07T14:28|2017-05-25T14:43|      www.cnn.com|
|  5|Alexander| 1.1666666666666665|2017-05-09T14:35|2017-05-25T14:40|  www.nytimes.com|
|  1|Alexander|0.23333333333333334|2017-05-16T14:58|2017-05-16T14:58|     www.espn.com|
|  1|Alexander|0.23333333333333334|2017-05-23T14:25|2017-05-23T14:25|   www.amazon.com|
|  1|Alexander|0.23333333333333334|2017-05-24T14:01|2017-05-24T14:01|  www.twitter.com|
|  4|   Andrew| 0.9333333333333333|2017-04-29T14:16|2017-05-22T14:49|    www.pixar.com|
|  1|   Andrew|0.23333333333333334|2017-04-30T14:25|2017-04-30T14:25|   www.google.com|
|  3|   Andrew| 0.7000000000000001|2017-04-30T14:49|2017-05-22T14:21|     www.espn.com|
|  5|   Andrew| 1.1666666666666665|2017-05-02T14:34|2017-05-25T14:36|  www.nytimes.com|
|  3|   Andrew| 0.7000000000000001|2017-05-03T14:56|2017-05-14T14:28|   www.amazon.com|
|  2|   Andrew| 0.4666666666666667|2017-05-04T14:35|2017-05-09T14:48|  www.twitter.com|
|  3|   Andrew| 0.7000000000000001|2017-05-07T14:34|2017-05-26T14:19|www.pinterest.com|
|  2|   Andrew| 0.4666666666666667|2017-05-10T14:04|2017-05-12T14:39| www.facebook.com|
|  1|   Andrew|0.23333333333333334|2017-05-12T14:19|2017-05-12T14:19|      www.cnn.com|
|  1|   Andrew|0.23333333333333334|2017-05-29T14:21|2017-05-29T14:21|    www.yahoo.com|
|  4|  Anthony| 0.9333333333333333|2017-04-29T14:10|2017-05-15T14:08|    www.yahoo.com|
|  5|  Anthony| 1.1666666666666665|2017-05-02T14:43|2017-05-21T14:06|www.pinterest.com|
|  6|  Anthony| 1.4000000000000001|2017-05-03T14:00|2017-05-27T14:53|  www.nytimes.com|
|  3|  Anthony| 0.7000000000000001|2017-05-07T14:51|2017-05-27T14:17|  www.twitter.com|
|  4|  Anthony| 0.9333333333333333|2017-05-11T14:19|2017-05-28T14:08| www.facebook.com|
|  2|  Anthony| 0.4666666666666667|2017-05-13T14:27|2017-05-17T14:13|   www.google.com|
|  1|  Anthony|0.23333333333333334|2017-05-18T14:16|2017-05-18T14:16|    www.pixar.com|
|  2|   Austin| 0.4666666666666667|2017-04-29T14:39|2017-05-23T14:37|      www.cnn.com|
|  3|   Austin| 0.7000000000000001|2017-05-01T14:47|2017-05-08T14:34|  www.nytimes.com|
|  2|   Austin| 0.4666666666666667|2017-05-03T14:27|2017-05-07T14:50|  www.twitter.com|
|  4|   Austin| 0.9333333333333333|2017-05-05T14:20|2017-05-26T14:43|www.pinterest.com|
|  1|   Austin|0.23333333333333334|2017-05-07T14:02|2017-05-07T14:02|     www.espn.com|
|  2|   Austin| 0.4666666666666667|2017-05-07T14:57|2017-05-19T14:40|    www.pixar.com|
|  3|   Austin| 0.7000000000000001|2017-05-15T14:09|2017-05-25T14:32| www.facebook.com|
|  3|   Austin| 0.7000000000000001|2017-05-15T14:22|2017-05-22T14:34|   www.amazon.com|
|  1|   Austin|0.23333333333333334|2017-05-16T14:58|2017-05-16T14:58|    www.yahoo.com|
|  8| Benjamin| 1.8666666666666667|2017-04-30T14:35|2017-05-28T14:24|  www.nytimes.com|
|  2| Benjamin| 0.4666666666666667|2017-05-01T14:24|2017-05-03T14:45|www.pinterest.com|
|  1| Benjamin|0.23333333333333334|2017-05-02T14:23|2017-05-02T14:23|    www.yahoo.com|
|  5| Benjamin| 1.1666666666666665|2017-05-03T14:56|2017-05-28T14:39|   www.google.com|
|  3| Benjamin| 0.7000000000000001|2017-05-06T14:27|2017-05-17T14:46|    www.pixar.com|
|  2| Benjamin| 0.4666666666666667|2017-05-08T14:34|2017-05-23T14:19|  www.twitter.com|
|  2| Benjamin| 0.4666666666666667|2017-05-13T14:22|2017-05-16T14:08|   www.amazon.com|
|  1| Benjamin|0.23333333333333334|2017-05-16T14:20|2017-05-16T14:20| www.facebook.com|
|  1| Benjamin|0.23333333333333334|2017-05-28T14:50|2017-05-28T14:50|     www.espn.com|
|  2|  Brandon| 0.4666666666666667|2017-05-06T14:20|2017-05-10T14:01|    www.pixar.com|
|  3|  Brandon| 0.7000000000000001|2017-05-10T14:16|2017-05-23T14:34| www.facebook.com|
|  3|  Brandon| 0.7000000000000001|2017-05-10T14:21|2017-05-22T14:08|   www.amazon.com|
|  3|  Brandon| 0.7000000000000001|2017-05-10T14:30|2017-05-20T14:58|  www.nytimes.com|
|  2|  Brandon| 0.4666666666666667|2017-05-16T14:24|2017-05-22T14:58|  www.twitter.com|
|  2|  Brandon| 0.4666666666666667|2017-05-17T14:33|2017-05-20T14:26|   www.google.com|
+---+---------+-------------------+----------------+----------------+-----------------+
only showing top 50 rows
```

### Create and Update Results (not functional yet)


There are some aspects to be considered when updating the data. The results table will always grow because the value of first visit can not be thrown away.

To update the reports periodically the steps are

1. generate a new report for the last 30 days
1. change the `first` visit to the older values that exist in the previous report
1. add old values which are not populated in new results and zero out the frequency and count
1. store new report

The scipt is included [here](./src/updateTable.py).

Due to some issues with namespace or perhaps sessions, the scripts need to be executed manually from a pyspark terminal.

Still missing is the initialization of the report table, which establishes the combination of domain_name and usr as the primary key. I would opt for a key-value store instead. Some iterations are still needed to verify if values are indeed being replaced.

## Some Thoughts

### Partitions
Source data should be partitioned by day, this would ensure that only 30 partitions would be consumed at any time.

The results table should be partitioned by either domain or user, depending on how the queries grow organically.

### Large Datasets
The table traversal could be done in a lazy way for the case where the combination of user:domain is massive and can't be held in memory. For that instance rather than using the table-like report, a key-value store is more suitable. I saw a few mentions in the spark documentation, however it was very cumbersome in practice.