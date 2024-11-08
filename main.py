"""
Main cli or app entry point
"""

from mylib.calculator import manage_spark, extract, load_data, transform

if __name__ == "__main__":
    # Initialize Spark session
    spark = manage_spark("DataProcessingApp")

    # Extract data
    data_path = extract()

    # Load data with the recent grads schema
    recent_grads_df = load_data(spark, data_path)

    # Transform data
    transformed_df = transform(spark, recent_grads_df)

    # Show transformed data (for debugging/testing purposes)
    transformed_df.show()

    # Stop Spark session
    manage_spark("DataProcessingApp", stop=True)
