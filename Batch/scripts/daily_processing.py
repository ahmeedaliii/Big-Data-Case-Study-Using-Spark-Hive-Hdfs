from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofmonth, month, year, quarter, substring_index, split, when, concat_ws, lit,round
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
import os

spark = SparkSession\
    .builder\
    .master("local[4]")\
    .appName("processing")\
    .config("spark.eventLog.logBlockUpdates.enabled", True)\
    .enableHiveSupport() \
    .getOrCreate()

# Get the current time
current_time = datetime.now()

# Subtract one year and one day
new_time = current_time - timedelta(days=367)

# Format the previous hour's directory path
year = new_time.strftime("%Y")
month = new_time.strftime("%m")
day = new_time.strftime("%d")

#Get the most selling products

sql_query = f"""
SELECT
    s.name AS sales_agent_name,
    p.product_name,
    SUM(sf.units) AS total_units_sold
FROM
    retail.sales_transactions_Fact sf
JOIN
    retail.product_dim p ON sf.product_id = p.product_id
JOIN
    retail.sales_agents_Dim s ON s.sales_person_id = sf.sales_agent_id
WHERE
    sf.day = '{day}' AND sf.year = '{year}' AND sf.month = '{month}'
GROUP BY
    s.name, p.product_name
ORDER BY
    total_units_sold DESC
"""
daily_report = spark.sql(sql_query)

# Define the output path where you want to save the CSV file
output_path = f"file:///data/project/daily_reports/{day}-{month}-{year}/"

# Write DataFrame to CSV
daily_report.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)