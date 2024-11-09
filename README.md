[![CI](https://github.com/nogibjj/Cindy_Gao_mini10_pyspark/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Cindy_Gao_mini10_pyspark/actions/workflows/cicd.yml)
# PySpark Data Processing


## Project Overview

The **PySpark Data Processing Project** is designed to process, transform, and analyze a dataset of recent college graduates using PySpark. The project incorporates various steps typical in data engineering and analytics workflows, such as data extraction, loading, transformation, and summarization. With Spark’s powerful processing capabilities, this project handles and transforms data efficiently, and it also includes testing, containerization, and CI/CD integration for reliability and reproducibility.
<br><br>
## Key Features

- **Data Extraction**: Downloads the dataset of recent graduates from a specified URL and saves it locally for processing.
- **Data Loading**: Loads the dataset into a Spark DataFrame using a predefined schema to ensure consistent data types.
- **Data Transformation**: Adds categorical transformations to the data, specifically creating a new column based on the `ShareWomen` metric, classifying the gender distribution within each major.
- **Spark SQL Integration**: Uses Spark SQL to perform more complex data filtering and aggregation operations on the dataset, specifically filtering and sorting based on employment totals.
- **Automated Reporting**: Generates a Markdown summary report of the transformed dataset for easy review and analysis.
<br><br>

## Raw Data Source
https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/recent-grads.csv
<br><br>

## Project file structure:
```plaintext
CINDY_GAO_MINI10_PYSPARK/
├── .devcontainer/
│   ├── devcontainer.json         # Dev container configuration
│   └── Dockerfile                # Docker configuration for the container
├── .github/workflows/
│   └── cicd.yml                  # GitHub Actions workflow for CI/CD
├── data/
│   └── recent-grads.csv          # Original dataset file (downloaded)
├── mylib/
│   └── calculator.py             # Core functions for data processing
├── output/
│   └── transformed-recent-grads.md  # Markdown report with transformed data
├── .gitignore
├── Dockerfile                    # Docker configuration for consistent environments
├── LICENSE                       # License for the project
├── Makefile                      # Build and test commands
├── README.md                     # Project documentation (this file)
├── main.py                       # Main script to run the data processing workflow
├── requirements.txt              # Python dependencies
└── test_main.py                  # Tests for functions in calculator.py
```
<br><br>


## Summary of calculator.py:
1. **`manage_spark`**: Initializes or stops a Spark session with optional memory settings.

2. **`extract`**: Downloads data from a URL and saves it to a file. It can also use test data for testing purposes.

3. **`load_data`**: Loads a CSV file into a Spark DataFrame using a predefined schema to ensure the correct data types.

4. **`transform`**: Adds a new column (`WomenProportion`) to the DataFrame, categorizing the `ShareWomen` column into quartiles (Very Low, Low, High, Very High).

5. **`Spark_SQL`**: Uses Spark SQL to filter and aggregate data. It selects major categories with a total employment count over 10,000 and sorts them in descending order by employment.

<br><br>

## Transformed Report Summary:
![image](https://github.com/user-attachments/assets/bf80cc88-a359-4c7e-a5f2-2360b1755ce8)

The new column is added as the last column in the markdown.






