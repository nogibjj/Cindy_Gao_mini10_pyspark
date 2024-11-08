"""
Calculations library
"""

import os
import requests
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, 
    StructField, 
    IntegerType, 
    StringType, 
    FloatType
)

# Manage Spark session with optional memory configuration and stop option
def manage_spark(app_name: str, memory: str = None, stop: bool = False) -> SparkSession:
    """Initialize or stop Spark session with optional memory setting."""
    builder = SparkSession.builder.appName(app_name)
    if memory:
        builder = builder.config("spark.executor.memory", memory)
    session = builder.getOrCreate()
    
    if stop:
        session.stop()
        return "stopped spark session"
    
    return session

# Extract data from URL and save it to a local file path
def extract(
    url, 
    file_path="data/recent-grads.csv", 
    directory="data", 
    test_content=None
):
    """Extract a URL to a specified file path or use test content if provided."""
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Write test content if provided, otherwise make a network call
    if test_content:
        content = test_content
    else:
        response = requests.get(url)
        content = response.content
        
    with open(file_path, "wb") as file:
        file.write(content)

    return file_path


def load_data(spark, data="data/recent-grads.csv"):
    """Load the Recent Grads dataset with the specified schema."""

    # Define schema for the Recent Grads dataset
    recent_grads_schema = StructType([
        StructField("Rank", IntegerType(), True),
        StructField("Major_code", IntegerType(), True),
        StructField("Major", StringType(), True),
        StructField("Total", IntegerType(), True),
        StructField("Men", IntegerType(), True),
        StructField("Women", IntegerType(), True),
        StructField("Major_category", StringType(), True),
        StructField("Sample_size", FloatType(), True),
        StructField("Employed", IntegerType(), True),
        StructField("Full_time", IntegerType(), True),
        StructField("Part_time", IntegerType(), True),
        StructField("Full_time_year_round", IntegerType(), True),
        StructField("Unemployed", IntegerType(), True),
        StructField("Unemployment_rate", FloatType(), True),
        StructField("Median", IntegerType(), True),
        StructField("P25th", IntegerType(), True),
        StructField("P75th", IntegerType(), True),
        StructField("College_jobs", IntegerType(), True),
        StructField("Non_college_jobs", IntegerType(), True),
        StructField("Low_wage_jobs", IntegerType(), True),
        StructField("ShareWomen", FloatType(), True)
    ])

    # Load data with the specified schema
    df = spark.read.option("header", "true").schema(recent_grads_schema).csv(data)
    
    return df

# Transform function to categorize 'ShareWomen' column based on quartiles
def transform(data: DataFrame) -> DataFrame:
    """Transform data by categorizing 'ShareWomen' into quartile-based categories."""
    # Calculate quartiles for the ShareWomen column
    q1, median, q3 = data.approxQuantile("ShareWomen", [0.25, 0.5, 0.75], 0.0)

    # Define conditions for categorizing ShareWomen based on quartiles
    conditions = [
        (F.col("ShareWomen") <= q1, "Very Low"),
        ((F.col("ShareWomen") > q1) & (F.col("ShareWomen") <= median), "Low"),
        ((F.col("ShareWomen") > median) & (F.col("ShareWomen") <= q3), "High"),
        (F.col("ShareWomen") > q3, "Very High")
    ]

    # Apply conditions to create GenderCategory4 column
    return data.withColumn("GenderCategory4", 
                           F.when(conditions[0][0], conditions[0][1])
                            .when(conditions[1][0], conditions[1][1])
                            .when(conditions[2][0], conditions[2][1])
                            .otherwise(conditions[3][1]))
