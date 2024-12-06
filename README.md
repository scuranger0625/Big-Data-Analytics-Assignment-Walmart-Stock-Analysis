Walmart Stock Analysis
This project analyzes Walmart stock data using PySpark in Google Colab, leveraging Spark DataFrame operations to extract insights and perform statistical calculations.

Project Overview
The analysis focuses on:

Exploring Walmart stock data.
Calculating statistics such as mean, max, and min.
Analyzing yearly and monthly trends in stock prices.
Investigating relationships between stock price and trading volume.
The dataset used for this analysis is walmart_stock.csv, which contains Walmart stock data from 2012 to 2017.

Setup Instructions
1. Upload the Dataset
Place the file walmart_stock.csv in your Google Drive.
2. Colab Setup
Open Google Colab and create a new notebook.
Mount your Google Drive:
python
複製程式碼
from google.colab import drive
drive.mount('/content/drive')
3. Install Required Libraries
Install PySpark in Colab:
python
複製程式碼
!apt-get -y install openjdk-8-jre-headless
!pip install pyspark
4. Load and Analyze the Data
Use the following code snippet to load and analyze the dataset:
python
複製程式碼
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, max, min, mean, corr, col, format_number

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Walmart Stock Analysis") \
    .getOrCreate()

# Load the dataset
file_path = '/content/drive/My Drive/walmart_stock.csv'  # Update with your file path
df = spark.read.csv(file_path, inferSchema=True, header=True)

# Perform analysis (e.g., descriptive statistics, correlations, trends)
df.describe().show()
Key Features
Descriptive Statistics:

Calculate and format mean, min, max, and standard deviation for key columns.
Stock Insights:

Count the days where closing price was below $60.
Find the percentage of days when the highest price exceeded $80.
Trend Analysis:

Determine yearly maximum stock prices.
Compute monthly average closing prices.
Correlation:

Calculate the Pearson correlation between High (stock price) and Volume (trading volume).
Execution Results
Example Output
Descriptive Statistics:

sql
複製程式碼
+-------+-----------------+------------------+------------------+-----------------+-------------------+--------------------+
|summary|             Open|              High|               Low|            Close|              Volume|
+-------+-----------------+------------------+------------------+-----------------+-------------------+--------------------+
|  count|             1000|              1000|              1000|             1000|                1000|
|   mean|           77.064|            79.078|            75.186|           77.188|        1170721.265|
+-------+-----------------+------------------+------------------+-----------------+-------------------+--------------------+
Yearly Maximum Prices:

sql
複製程式碼
+----+----------+
|Year|max(High) |
+----+----------+
|2012|  77.87875|
|2013|  81.37000|
+----+----------+
How to Run
Copy the code provided above into a Google Colab notebook.
Follow the setup steps and upload the dataset to your Google Drive.
Execute each cell step-by-step to reproduce the analysis.
