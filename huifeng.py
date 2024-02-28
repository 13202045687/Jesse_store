#!/usr/bin/env python
# @desc : 
__coding__ = "utf-8"
__author__ = "bytedance"

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# Create a Spark session
spark = SparkSession.builder.appName("year_count").getOrCreate()

# Given data
data = [
    ('ABC17969(AB)', '1', 'ABC17969', 2022),
    ('ABC17969(AB)', '2', 'CDC52533', 2022),
    ('ABC17969(AB)', '3', 'DEC59161', 2023),
    ('ABC17969(AB)', '4', 'F43874', 2022),
    ('ABC17969(AB)', '5', 'MY06154', 2021),
    ('ABC17969(AB)', '6', 'MY4387', 2022),

    ('AE686(AE)', '7', 'AE686', 2023),
    ('AE686(AE)', '8', 'BH2740', 2021),
    ('AE686(AE)', '9', 'EG999', 2021),
    ('AE686(AE)', '10', 'AE0908', 2021),
    ('AE686(AE)', '11', 'QA402', 2022),
    ('AE686(AE)', '12', 'OM691', 2022)
]

# Create a DataFrame
df = spark.createDataFrame(data, ["peer_id", "id_1", "id_2", "year"])

# Step 1: Get the year when peer_id contains id_2
step1_df = df.groupBy("peer_id").agg(F.max("year").alias("max_year"))

step1_df.show()

# Step 2: Count the number of each year and filter based on the given size
size_number = 3
step2_df = df.join(step1_df, on="peer_id", how="inner") \
    .filter("year <= max_year") \
    .groupBy("peer_id", "year") \
    .agg(F.count("*").alias("count")) \
    .orderBy("year")

step2_df.show()

# Step 3: Calculate the cumulative count and filter based on the given size
windowSpec = Window.partitionBy("peer_id").orderBy(F.desc("year")).rangeBetween(Window.unboundedPreceding, 0)
step3_df = step2_df.withColumn("cumulative_count", F.sum("count").over(windowSpec)) \
    .filter("cumulative_count >= " + str(size_number)) \
    .select("peer_id", "year")

# Show the final result
step3_df.show()
