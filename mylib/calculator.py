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

def extract(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/recent-grads.csv",
    file_path="data/recent-grads.csv",
    directory="data",
):
    """Extract a URL to a specified file path."""
    if not os.path.exists(directory):
        os.makedirs(directory)

    response = requests.get(url)
    response.raise_for_status()  
    with open(file_path, "wb") as file:
        file.write(response.content)

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

def Spark_SQL(data: DataFrame) -> DataFrame:
    """Filter categories with total employment over 10,000 using Spark SQL."""
    # Register the DataFrame as a temporary view
    data.createOrReplaceTempView("recent_grads")

    # Use Spark SQL to calculate total employment per category and filter results
    query = """
    SELECT 
        Major_category,
        SUM(Employed) AS Total_Employed
    FROM recent_grads
    GROUP BY Major_category
    HAVING Total_Employed > 10000
    ORDER BY Total_Employed DESC
    """

    # Execute the query and return the resulting DataFrame
    result_df = data.sql_ctx.sql(query)
    return result_df

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

    # Apply conditions to create WomenProportion column
    return data.withColumn("WomenProportion", 
                           F.when(conditions[0][0], conditions[0][1])
                            .when(conditions[1][0], conditions[1][1])
                            .when(conditions[2][0], conditions[2][1])
                            .otherwise(conditions[3][1]))
