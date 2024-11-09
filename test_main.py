"""
Test goes here

"""
import os
import pytest
from pyspark.sql import SparkSession
from mylib.calculator import manage_spark, extract, load_data, Spark_SQL, transform
from main import save_to_markdown  # Import the function from main.py
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
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
    """Test manage_spark function for initialization and stopping."""
    session = manage_spark("TestApp")
    assert isinstance(session, SparkSession)
    assert session.sparkContext.appName == "TestApp"
    result = manage_spark("TestApp", stop=True)
    assert result == "stopped spark session"


def test_extract():
    """Test the extract function by downloading the file from URL."""
    test_directory = "data"
    test_file_path = f"{test_directory}/recent-grads.csv"
    url = (
        "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/"
        "master/college-majors/recent-grads.csv"
    )

    # Use the extract function to download the file
    file_path = extract(url=url, file_path=test_file_path, directory=test_directory)

    # Verify that the file was successfully created
    assert os.path.exists(file_path), "File was not created by extract function."

    # Remove the file after the test to keep the directory clean
    if os.path.exists(test_file_path):
        os.remove(test_file_path)


def test_load_data(spark):
    """Test loading the Recent Grads data to ensure correct schema and data loading."""
    # Use extract to download the file
    data_path = extract(
        url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/"
        "master/college-majors/recent-grads.csv",
        file_path="data/recent-grads.csv",
        directory="data",
    )

    # Load data and check schema and row count
    df = load_data(spark, data_path)
    assert df.count() > 0, "Data count does not match expected count"
    assert df.schema == StructType(
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
    ), "Schema does not match expected schema"

    # # Remove the file after the test to keep the directory clean
    # if os.path.exists(data_path):
    #     os.remove(data_path)


def test_Spark_SQL(spark):
    """Test the Spark_SQL function to ensure it
    filters and sorts categories correctly."""

    # Create a sample DataFrame
    data = [
        (1, "Engineering", 12000),
        (2, "Humanities & Liberal Arts", 5000),
        (3, "Biology & Life Science", 15000),
        (4, "Health", 8000),
        (5, "Physical Sciences", 11000),
    ]
    schema = StructType(
        [
            StructField("Rank", IntegerType(), True),
            StructField("Major_category", StringType(), True),
            StructField("Employed", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)

    # Apply the Spark_SQL function
    result_df = Spark_SQL(df)

    # Collect results to verify
    results = result_df.collect()

    # Check if the number of rows returned is correct
    # (should only include rows with Total_Employed > 10000)
    assert len(results) == 3, "Expected 3 rows with Total_Employed > 10000"

    # Verify the categories and sorting order by Total_Employed
    assert (
        results[0]["Major_category"] == "Biology & Life Science"
    ), "Expected 'Biology & Life Science' as the first category"
    assert (
        results[1]["Major_category"] == "Engineering"
    ), "Expected 'Engineering' as the second category"
    assert (
        results[2]["Major_category"] == "Physical Sciences"
    ), "Expected 'Physical Sciences' as the third category"
    assert (
        results[0]["Total_Employed"] > results[1]["Total_Employed"]
    ), "Expected Total_Employed of first result to be greater than second"
    assert (
        results[1]["Total_Employed"] > results[2]["Total_Employed"]
    ), "Expected Total_Employed of second result to be greater than third"


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

    # Check that the `WomenProportion` column was added and has expected values
    categories = [
        row["WomenProportion"]
        for row in transformed_df.select("WomenProportion").collect()
    ]
    assert "Very Low" in categories, "Missing 'Very Low' category"
    assert "Low" in categories, "Missing 'Low' category"
    assert "High" in categories, "Missing 'High' category"
    assert "Very High" in categories, "Missing 'Very High' category"


def test_save_to_markdown(spark):
    """Test save_to_markdown function to ensure
    it generates a markdown file with transformed data."""
    # Create a sample DataFrame for testing
    data = [(1, 0.1), (2, 0.3), (3, 0.5), (4, 0.7), (5, 0.9)]
    schema = StructType(
        [
            StructField("ID", IntegerType(), True),
            StructField("ShareWomen", FloatType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)

    # Apply transformation
    transformed_df = transform(df)

    # Define the output markdown file path
    file_path = "output/test_report.md"
    save_to_markdown(transformed_df, file_path)

    # Verify the markdown file was created
    assert os.path.exists(file_path), "Markdown file was not created."

    # Optionally, read the file and verify content if needed
    with open(file_path, "r") as file:
        content = file.read()
        assert "Transformed Dataset" in content, "Markdown content is missing header."
        assert (
            "Very Low" in content
        ), "Transformed data content is missing expected category."

    # Clean up the test file after verification
    if os.path.exists(file_path):
        os.remove(file_path)
