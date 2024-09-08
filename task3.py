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
from pyspark.sql.functions import col, from_unixtime, month, desc, sum as _sum, row_number, concat, lit, to_date
from pyspark.sql.window import Window
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("NYC Rideshare Analysis Task 3")\
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

    # window specification for ranking within each month
    pickupWindowSpec = Window.partitionBy("Month").orderBy(desc("trip_count"))
    dropoffWindowSpec = Window.partitionBy("Month").orderBy(desc("trip_count"))

    # top 5 popular pickup boroughs each month
    top_pickup_boroughs = (merged_rideshare_df
                       .withColumn("Month", month("date"))
                       .groupBy("Pickup_Borough", "Month")
                       .count()
                       .withColumnRenamed("count", "trip_count")
                       .withColumn("rank", row_number().over(pickupWindowSpec))
                       .filter(col("rank") <= 5)
                       .orderBy("Month", "rank"))

    top_pickup_boroughs.show(50, truncate=False)  # Show more rows to verify the results for each month

    # top 5 popular dropoff boroughs each month
    top_dropoff_boroughs = (merged_rideshare_df
                        .withColumn("Month", month("date"))
                        .groupBy("Dropoff_Borough", "Month")
                        .count()
                        .withColumnRenamed("count", "trip_count")
                        .withColumn("rank", row_number().over(dropoffWindowSpec))
                        .filter(col("rank") <= 5)
                        .orderBy("Month", "rank"))

    top_dropoff_boroughs.show(50, truncate=False)  # Show more rows to verify the results for each month
    
    # top 30 earnest routes based on total profits
    top_routes = (merged_rideshare_df
              .withColumn("Route", concat(col("Pickup_Borough"), lit(" to "), col("Dropoff_Borough")))
              .groupBy("Route")
              .agg(_sum("driver_total_pay").alias("total_profit"))
              .orderBy(desc("total_profit"))
              .limit(30))

    top_routes.show(truncate=False)


    spark.stop()