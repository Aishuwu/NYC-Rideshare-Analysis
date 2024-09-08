import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, month, sum, count, col
from pyspark.sql.types import IntegerType
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("NYC Rideshare Analysis Task 2")\
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


    # Print the number of rows and schema of the final dataframe
    # print("Number of rows:", merged_rideshare_df.count())
    # merged_rideshare_df.printSchema()

    merged_rideshare_df = merged_rideshare_df.withColumn("rideshare_profit", col("rideshare_profit").cast(IntegerType())) \
           .withColumn("driver_total_pay", col("driver_total_pay").cast(IntegerType())) \
           .withColumn("month", month(col("date").cast("date")))  # extracting month from date for grouping

    # aggregating data by business and month
    trips_per_month = merged_rideshare_df.groupBy("business", "month").count()
    profits_per_month = merged_rideshare_df.groupBy("business", "month").agg(sum("rideshare_profit").alias("total_profit"))
    earnings_per_month = merged_rideshare_df.groupBy("business", "month").agg(sum("driver_total_pay").alias("total_earnings"))


    # saving the trips_per_month, profits_per_month and earnings_per_month data to the s3 bucket
    trips_per_month.coalesce(1).write.option("header", "true").mode("overwrite").csv(f"s3a://{s3_bucket}/task2/trips_per_month")


    profits_per_month.coalesce(1).write.option("header", "true").mode("overwrite").csv(f"s3a://{s3_bucket}/task2/profits_per_month")

    
    earnings_per_month.coalesce(1).write.option("header", "true").mode("overwrite").csv(f"s3a://{s3_bucket}/task2/earnings_per_month")


    spark.stop()



