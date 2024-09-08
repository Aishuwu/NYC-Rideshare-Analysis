import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, month
from pyspark.sql.functions import to_date, count, col, avg, dayofmonth
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("NYC Rideshare Analysis Task 5")\
        .getOrCreate()


    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # rideshare_data = # /read from the bucket file
    # taxi_zone_lookup_df = # /read from the bucket file

    # paths to the datasets
    rideshare_data_path = "s3a://data-repository-bkt/ECS765/rideshare_2023/rideshare_data.csv"
    taxi_zone_lookup_path = "s3a://data-repository-bkt/ECS765/rideshare_2023/taxi_zone_lookup.csv"

    # loading the datasets
    rideshare_data_df = spark.read.option("header", "true").csv(rideshare_data_path)
    taxi_zone_lookup_df = spark.read.option("header", "true").csv(taxi_zone_lookup_path)

    # joining the datasets
    # first join on pickup_location
    first_join = rideshare_data_df.join(taxi_zone_lookup_df, rideshare_data_df.pickup_location == taxi_zone_lookup_df.LocationID)\
        .withColumnRenamed("Borough", "Pickup_Borough")\
        .withColumnRenamed("Zone", "Pickup_Zone")\
        .withColumnRenamed("service_zone", "Pickup_service_zone")

    # dropping the LocationID column 
    first_join = first_join.drop("LocationID")

    # second join on dropoff_location
    merged_rideshare_df = first_join.join(taxi_zone_lookup_df, first_join.dropoff_location == taxi_zone_lookup_df.LocationID)\
        .withColumnRenamed("Borough", "Dropoff_Borough")\
        .withColumnRenamed("Zone", "Dropoff_Zone")\
        .withColumnRenamed("service_zone", "Dropoff_service_zone")

    
    # dropping the second LocationID column to finalize the dataframe
    merged_rideshare_df = merged_rideshare_df.drop("LocationID")

    
    # converting the date column from UNIX timestamp to yyyy-MM-dd
    merged_rideshare_df = merged_rideshare_df.withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))
    merged_rideshare_df = merged_rideshare_df.withColumn("date", to_date(merged_rideshare_df.date, "yyyy-MM-dd"))


    # Extracting January data
    january_data = rideshare_data_df.filter(month("date") == 1)

    # Calculating average waiting time per day for January
    average_waiting_time_per_day = january_data.groupBy(dayofmonth("date").alias("day"))\
        .agg(avg("request_to_pickup").alias("average_waiting_time"))\
        .orderBy("day")

    # Saving the result to S3
    average_waiting_time_per_day.coalesce(1).write.option("header", "true").mode("overwrite")\
        .csv(f"s3a://{s3_bucket}/task5/average_waiting_time_per_day")

    spark.stop()
