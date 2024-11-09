"""
Main CLI or app entry point
"""

import os
from mylib.calculator import manage_spark, extract, load_data, Spark_SQL, transform


def save_to_markdown(df, file_path="output/report.md"):
    """Save the entire transformed Spark DataFrame as a Markdown file."""
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Save the entire transformed dataset as a Markdown table
    with open(file_path, "w") as file:
        file.write("# Transformed Data Report\n\n")
        file.write("## Transformed Dataset\n\n")
        file.write(pandas_df.to_markdown(index=False))
    print(f"Markdown report saved to {file_path}")


if __name__ == "__main__":
    # Initialize Spark session
    spark = manage_spark("DataProcessingApp")

    # Extract data from URL to local file path
    data_path = extract()

    # Load data with the recent grads schema
    recent_grads_df = load_data(spark, data_path)

    # Filter categories with high employment using Spark SQL
    high_employment_categories_df = Spark_SQL(recent_grads_df)

    # Transform the entire dataset
    transformed_df = transform(recent_grads_df)

    # Show transformed data (for debugging/testing purposes)
    transformed_df.show()

    # Save the entire transformed dataset as a Markdown report
    save_to_markdown(transformed_df, "output/transformed-recent-grads.md")

    # Stop Spark session
    manage_spark("DataProcessingApp", stop=True)
