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

1. build a table of Recency and Frequency of each user visiting a domain
1. table should refresh daily once a day
1. query for each user domain combo
  - frequency of the visits in last 30 days
  - when was the first visit (lifetime)
  - when was the first last (lifetime)
1. implement using Spark2.0 or Hive SQL

### Parsing Requirements

#### 1 Table

My first thoughts on this table are drawn from a look at frequency. From my background in numerical analysis I think of a Fourier or Laplace transforms. However, looking at the requirements, `frequency of the visits in the last 30 days` could be reported simply as a daily average (visits/day). I believe simple is always better than complex; it allows for a quicker turn-around time that presents relevant information. Once it is understood that this is useful, discussions can be had about how precise & accurate the value needs to be.

#### 2 Daily Update

When considering periodic updates the first thing that comes to mind is a cron job. However, depending on the infrastructure, it cold be accomplished in many ways, such as:

* cron
* AWS Lambda recurring function
* if batch jobs are already scheduled in hadoop, this could be just one more `jar` to execute
* presumably, Spark may also have a feature to handle this task

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

### Methodology

It's best to try and fail on a tight feedback loop. To accomplish this, setting up a small Spark dev environment would allow for real testing and feedback.

A quick search for Spark 2.0 on docker revealed an accessible version of [Apache Spark on Docker](https://hub.docker.com/r/aghorbani/spark/).

#### Dev Env

To setup the container environment:

##### Setup Container
```
> git clone git@github.com:a-ghorbani/docker-spark.git
> cd docker-spark
> docker pull aghorbani/spark:2.1.0
> docker build --rm -t aghorbani/spark:2.1.0 .
```
##### Start Container
```
> docker run \
 -v ./src:/data \
 -it \
 -p 8088:8088 \
 -p 8042:8042 \
 -p 4040:4040 \
 -h sandbox aghorbani/spark:2.1.0 \
 bash
```

#### Test Data

With a working environment for executing jobs/queries, it follows that test data is generated. The following characteristics were considered when generating test data

* collisions in time stamp are possible
* data may not arrive in chronological order
* combine list of domains with list of users (user == user_id)
* use random integers to generate paths of url
* for this exercise `STRING` timestamps are sufficient as the comparator operators (`>`, erc.) work with ISO-8601 dates

##### Code

A simple [python script](./src/generateData.py) was used to generate the data.
