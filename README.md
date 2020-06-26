# SPARKIFY DATA LAKE USING SPARK

## INTRODUCTION

Sparkify, a music streaming company, decided to adopt concept of a data lake and data lake technology.
Luckily, the data about logs and songs are already stored in an AWS S3 bucket.

Our task as data engineers is to establish data lake principles, and for that we will be using Spark.

## PROJECT DESCRIPTION

This project require running a spark job that will pull the required data from an S3 bucket and transform it into appropriate fact/dimension tables stored as parquet files.
Creating an Elastic MapReduce cluster is the fist step, then uploading the spark job code to the master node. Finally, run the spark job to pull the data and store the fact/dimension tables.
Now everything is ready for the analytics team.

## DATA SCHEMA

- **songplays** - Fact table containing records in log data associated with song plays

- **users** - Dimension table containing information about the users in the app

- **songs** - Dimension table containing the songs library

- **artists** - Dimension table containing artists library

- **time** - Dimension table containing timestamps of records broken down into specific units


## PROJECT STRUCTURE
The data is stored on an S3 bucket on AWS.
The configuration file, `dl.cfg`, has all the information required to connect to AWS;

`etl.py` contains the spark ETL pipeline to pull data from s3 buckets, transform it, then store it as parquet files.

`etl_local.ipynb` is used to test the ETL logic locally before uploading it to Amazon Elastic MapReduce.


To run script:
1. Make sure you have an Elastic MapReduce cluster running.
2. Upload etl.py to the master node.
3. Run etl.py to activate the pipeline.