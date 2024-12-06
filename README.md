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
Upload the Dataset
Place the file walmart_stock.csv in your Google Drive.

Colab Setup
Open Google Colab and create a new notebook.
Mount your Google Drive:

python
複製程式碼
from google.colab import drive
drive.mount('/content/drive')
Install Required Libraries
Install PySpark in Colab:

python
複製程式碼
!apt-get -y install openjdk-8-jre-headless
!pip install pyspark
Load and Analyze the Data
Use PySpark to load and analyze the dataset from Google Drive.

Key Features
Descriptive Statistics:

Calculate and format mean, min, max, and standard deviation for key columns.
Stock Insights:

Count the days where the closing price was below $60.
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
Open Google Colab and follow the setup instructions.
Upload the dataset to your Google Drive.
Execute the code step-by-step to complete the analysis.
中文版
沃爾瑪股票分析
此專案使用 PySpark 在 Google Colab 上分析沃爾瑪股票數據，通過 Spark DataFrame 操作來提取見解並執行統計計算。

專案概述
此分析的重點包括：

探索沃爾瑪股票數據。
計算統計數據（例如平均值、最大值和最小值）。
分析股票價格的年度和月度趨勢。
調查股票價格與交易量之間的關係。
數據集使用的是 walmart_stock.csv，包含沃爾瑪在 2012 至 2017 年的股票數據。

設置說明
上傳數據集
將文件 walmart_stock.csv 上傳到您的 Google Drive。

Colab 設置
打開 Google Colab 並創建一個新的 Notebook。
掛載您的 Google Drive：

python
複製程式碼
from google.colab import drive
drive.mount('/content/drive')
安裝必要的庫
在 Colab 中安裝 PySpark：

python
複製程式碼
!apt-get -y install openjdk-8-jre-headless
!pip install pyspark
加載並分析數據
使用 PySpark 從 Google Drive 加載並分析數據集。

主要功能
描述性統計：

計算並格式化主要列的平均值、最小值、最大值和標準差。
股票見解：

計算收盤價低於 $60 的天數。
計算最高價超過 $80 的天數佔總天數的百分比。
趨勢分析：

確定每年的最高股票價格。
計算每月的平均收盤價。
相關性：

計算最高股票價格（High）與交易量（Volume）之間的皮爾森相關係數。
執行結果
範例輸出
描述性統計：

sql
複製程式碼
+-------+-----------------+------------------+------------------+-----------------+-------------------+--------------------+
|summary|             Open|              High|               Low|            Close|              Volume|
+-------+-----------------+------------------+------------------+-----------------+-------------------+--------------------+
|  count|             1000|              1000|              1000|             1000|                1000|
|   mean|           77.064|            79.078|            75.186|           77.188|        1170721.265|
+-------+-----------------+------------------+------------------+-----------------+-------------------+--------------------+
年度最高價：

sql
複製程式碼
+----+----------+
|Year|max(High) |
+----+----------+
|2012|  77.87875|
|2013|  81.37000|
+----+----------+
執行方法
打開 Google Colab，按照設置說明進行操作。
將數據集上傳到您的 Google Drive。
按步驟執行代碼，完成分析。
