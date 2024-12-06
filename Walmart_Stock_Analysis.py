
# 引入必要的 PySpark 模組
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    format_number, mean, min, max, year, month, corr
)

# 初始化 Spark Session
spark = SparkSession.builder \
    .appName("Walmart Stock Analysis") \
    .getOrCreate()

# 讀取數據集
file_path = 'walmart_stock.csv'  # 替換為正確的文件路徑
df = spark.read.csv(file_path, inferSchema=True, header=True)

# 查看數據的基本信息
print("列名稱：", df.columns)  # 列出所有列名稱
df.printSchema()  # 顯示數據框架的結構

# 查看數據的前幾行
print("前 5 行數據：")
df.show(5)

# 將描述性統計數據中的數字格式化為兩位小數
summary = df.describe()
summary.select(
    summary['summary'],
    format_number(summary['Open'].cast('float'), 2).alias('Open'),
    format_number(summary['High'].cast('float'), 2).alias('High'),
    format_number(summary['Low'].cast('float'), 2).alias('Low'),
    format_number(summary['Close'].cast('float'), 2).alias('Close'),
    format_number(summary['Volume'].cast('int'), 0).alias('Volume')
).show()

# 新增 HV Ratio 列：每日最高價與成交量的比率
df_hv = df.withColumn('HV Ratio', df['High'] / df['Volume']).select(['HV Ratio'])
df_hv.show()

# 查詢最高價出現的日期
peak_high_date = df.orderBy(df['High'].desc()).select(['Date']).head(1)[0]['Date']
print(f"最高價出現的日期：{peak_high_date}")

# 計算收盤價的平均值
df.select(mean('Close').alias('Mean Close')).show()

# 查詢成交量的最大值和最小值
df.select(max('Volume').alias('Max Volume'), min('Volume').alias('Min Volume')).show()

# 計算收盤價低於 60 美元的天數
days_below_60 = df.filter(df['Close'] < 60).count()
print(f"收盤價低於 60 美元的天數：{days_below_60}")

# 計算最高價超過 80 美元的天數佔比
percentage_high_above_80 = df.filter('High > 80').count() * 100 / df.count()
print(f"最高價超過 80 美元的百分比：{percentage_high_above_80:.2f}%")

# 計算最高價與成交量的皮爾森相關係數
df.select(corr('High', 'Volume').alias('Correlation High-Volume')).show()

# 查詢每年的最高價
year_df = df.withColumn('Year', year(df['Date']))
year_df.groupBy('Year').max('High').select(
    'Year', 'max(High)'
).orderBy('Year').show()

# 計算每月的平均收盤價
month_df = df.withColumn('Month', month(df['Date']))
month_avg_close = month_df.groupBy('Month').mean('Close').select(
    'Month', 'avg(Close)'
).orderBy('Month')
month_avg_close.show()
