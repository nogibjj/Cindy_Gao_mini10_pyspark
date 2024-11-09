[![CI](https://github.com/nogibjj/Cindy_Gao_mini10_pyspark/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Cindy_Gao_mini10_pyspark/actions/workflows/cicd.yml)
# PySpark Data Processing

## Purpose of the project:
* Use PySpark to perform data processing on a large dataset
* Include at least one Spark SQL query and one data transformation
<br><br>

## Raw Data Source:
https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/recent-grads.csv
<br><br>

## Project file structure:
```plaintext
CINDY_GAO_MINI10_PYSPARK/
├── __pycache__/
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .github/
│   └── workflows/
│       └── cicd.yml
├── .pytest_cache/
├── .ruff_cache/
├── data/
│   └── recent-grads.csv
├── mylib/
│   ├── __pycache__/
│   ├── __init__.py
│   └── calculator.py  #Python module defining core functionality
├── output/
│   └── transformed-recent-grads.md  #The report in Markdown format after data transformation
├── .coverage
├── .gitignore
├── Dockerfile
├── LICENSE
├── Makefile
├── README.md
├── main.py  #Main entry point for the project
├── repeat.sh
├── requirements.txt
├── setup.sh
└── test_main.py  #Test file used to verify project functionality
```
<br><br>

## Summary of calculator.py:
1. **`manage_spark`**: Initializes or stops a Spark session with optional memory settings.

2. **`extract`**: Downloads data from a URL and saves it to a file. It can also use test data for testing purposes.

3. **`load_data`**: Loads a CSV file into a Spark DataFrame using a predefined schema to ensure the correct data types.

4. **`transform`**: Adds a new column (`WomenProportion`) to the DataFrame, categorizing the `ShareWomen` column into quartiles (Very Low, Low, High, Very High).
<br><br>

## Transformed Report Summary:
![image](https://github.com/user-attachments/assets/bf80cc88-a359-4c7e-a5f2-2360b1755ce8)

The new column is added as the last column in the markdown.






