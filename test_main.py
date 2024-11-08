"""
Test goes here

"""
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)
import pyspark.sql.functions as F
from mylib.calculator import manage_spark, extract, load_data, transform

# Set up a schema to use in the tests
recent_grads_schema = StructType(
    [
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
        StructField("ShareWomen", FloatType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark():
    """Fixture to initialize and tear down a Spark session for testing."""
    spark_session = (
        SparkSession.builder.appName("TestSession").master("local[*]").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_manage_spark():
    """Test manage_spark function for both initialization and stopping."""
    session = manage_spark("TestApp")
    assert isinstance(session, SparkSession)
    assert session.sparkContext.appName == "TestApp"
    result = manage_spark("TestApp", stop=True)
    assert result == "stopped spark session"


def test_extract():
    """Test the extract function without a network call by using test content."""
    test_directory = "data"
    test_file_path = f"{test_directory}/recent-grads.csv"

    # Define sample CSV content
    test_content = (
        b"Rank,Major_code,Major,Total,Men,Women,Major_category\n"
        b"1,1101,Engineering,5000,3000,2000,STEM\n"
    )

    # Run the extract function with test content
    file_path = extract(
        url="dummy_url",
        file_path=test_file_path,
        directory=test_directory,
        test_content=test_content,
    )

    # Assert the file was created and contains the expected content
    assert os.path.exists(file_path), "File was not created"
    with open(file_path, "rb") as f:
        content = f.read()
    assert content == test_content, "File content does not match expected content"

    # Clean up test file
    if os.path.exists(test_file_path):
        os.remove(test_file_path)


def test_load_data(spark):
    """Test loading the Recent Grads data to ensure correct schema and data loading."""

    # Define file path and sample data
    test_directory = "data"
    data_path = f"{test_directory}/test-recent-grads.csv"
    sample_data = (
        "Rank,Major_code,Major,Total,Men,Women,Major_category,Sample_size,"
        "Employed,Full_time,Part_time,Full_time_year_round,Unemployed,"
        "Unemployment_rate,Median,P25th,P75th,College_jobs,Non_college_jobs,"
        "Low_wage_jobs,ShareWomen\n"
        "1,1101,Engineering,5000,3000,2000,STEM,100,4800,4000,800,3500,200,"
        "0.04,60000,50000,70000,3000,1500,500,0.4\n"
    )

    # Ensure the data directory exists without deleting it if it has pre-existing files
    os.makedirs(test_directory, exist_ok=True)

    # Write sample data to a test CSV file
    with open(data_path, "w") as f:
        f.write(sample_data)

    # Load data and check the schema and row count
    df = load_data(spark, data_path)
    assert df.count() == 1, "Data count does not match expected count"
    assert (
        df.where(F.col("Major") == "Engineering").count() == 1
    ), "Expected row not found in DataFrame"

    # Clean up the test file without deleting the `data` directory
    if os.path.exists(data_path):
        os.remove(data_path)


def test_transform(spark):
    """Test the transform function to ensure correct quartile categorization."""
    # Create a sample DataFrame with `ShareWomen` values
    data = [(1, 0.1), (2, 0.3), (3, 0.5), (4, 0.7), (5, 0.9)]
    schema = StructType(
        [
            StructField("ID", IntegerType(), True),
            StructField("ShareWomen", FloatType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)

    # Apply the transform function
    transformed_df = transform(df)

    # Check that the `GenderCategory4` column was added and has expected values
    categories = [
        row["GenderCategory4"]
        for row in transformed_df.select("GenderCategory4").collect()
    ]
    assert "Very Low" in categories, "Missing 'Very Low' category"
    assert "Low" in categories, "Missing 'Low' category"
    assert "High" in categories, "Missing 'High' category"
    assert "Very High" in categories, "Missing 'Very High' category"
