from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofmonth, month, year, quarter, substring_index, split, when, concat_ws, lit,round,current_date,concat,lit
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
import os


# Initialize Spark session
spark = SparkSession\
    .builder\
    .appName("processing")\
    .master("yarn")\
    .config("spark.submit.deployMode", "client")\
    .config("spark.executor.instances", "2")\
    .config("spark.executor.memory", "2g")\
    .config("spark.executor.cores", "2")\
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.cores", "2") \
    .config("spark.eventLog.logBlockUpdates.enabled", True)\
    .enableHiveSupport()\
    .getOrCreate()


########################Reading from the dynamic Directory#############################

# Get the current time and subtract one hour
current_time = datetime.now()
previous_hour_time = current_time - timedelta(hours=1)

# Format the previous hour's directory path
base_path = "/retail_data/"
year = previous_hour_time.strftime("%Y")
month = previous_hour_time.strftime("%m")
day = previous_hour_time.strftime("%d")
hour = previous_hour_time.strftime("%H")

previous_hour_path = f"{base_path}year={year}/month={month}/day={day}/hour={hour}"

# Define the filename patterns with wildcard for batch number
branches_pattern = os.path.join(previous_hour_path, "branches*.csv")
sales_agents_pattern = os.path.join(previous_hour_path, "sales_agents*.csv")
sales_transactions_pattern = os.path.join(previous_hour_path, "sales_transactions*.csv")

############## Read branches CSV files into DataFrame###########################

branches_Dim = spark.read.csv(branches_pattern, header=True, inferSchema=True)

# Change the data type of the establish_date column to DateType
branches_Dim = branches_Dim.withColumn("establish_date", col("establish_date").cast(DateType()))

################### Read sales agents CSV files into DataFrame############################

sales_agents_Dim = spark.read.csv(sales_agents_pattern, header=True, inferSchema=True)
# Change the data type of the value column to DateType
sales_agents_Dim = sales_agents_Dim.withColumn("hire_date", col("hire_date").cast(DateType()))

###########3Read sales transactions CSV files into DataFrame####################

sales_transactions = spark.read.csv(sales_transactions_pattern, header=True, inferSchema=True)



########################CLeaning and Preparing Data #############################

#Rename some columns
sales_transactions = sales_transactions.withColumnRenamed("cusomter_lname", "customer_lname") \
                                       .withColumnRenamed("cusomter_email", "customer_email")
                                       
#Cleaning Email column
sales_transactions = sales_transactions.withColumn("customer_email",concat(substring_index("customer_email", ".com", 1), lit(".com")))
                                       
                                       
########################Selecting columns for customer_Dim ########################

customer_Dim = sales_transactions.select(
    col('customer_id'),
    col('customer_fname'),
    col('customer_lname'),
    col('customer_email')
).dropDuplicates()


########################Handle Location_Dim########################

# Split the 'shipping_address' column
split_col = split(sales_transactions['shipping_address'], '/')

# Create 'city' and 'state' columns
df = sales_transactions.withColumn('city', split_col.getItem(1)) \
                       .withColumn('state', split_col.getItem(2))

# Select columns for location_Dim DataFrame
df = df.select('city', 'state')

# Drop rows with null values
df = df.na.drop()

# Drop duplicate rows
df = df.dropDuplicates()

# Create 'location_id' in the format 'city-state'
df = df.withColumn("location_id", concat_ws("-", col("city"), col("state")))

# Selecting columns for location_Dim DataFrame
location_Dim = df.select(
    col('location_id'),
    col('city'),
    col('state')
)

#########################Handle Payment_Dim########################

# Select columns for Payment_Dim DataFrame
df = sales_transactions.select('is_online', 'payment_method')

# Drop rows with null values
df = df.na.drop()

# Drop duplicate rows
df = df.dropDuplicates()

# Create 'payment_id' in the format 'isonline-payment'
df = df.withColumn("payment_id", concat_ws("-", col("is_online"), col("payment_method")))

# Selecting columns for location_Dim DataFrame
payment_Dim = df.select(
    col('payment_id'),
    col('is_online'),
    col('payment_method')
)


#########################Handle Product_Dim########################

product_Dim = sales_transactions.select("product_id", "product_name", "product_category").dropDuplicates()

#########################Handle offer_Dim########################
df = sales_transactions.select('offer_1', 'offer_2','offer_3','offer_4','offer_5')

# Unpivot (melt) the DataFrame
unpivot_expr = "stack(5, 'offer_1', offer_1, 'offer_2', offer_2, 'offer_3', offer_3, 'offer_4', offer_4, 'offer_5', offer_5) as (offer_name, offer_value)"
unpivoted_df = df.selectExpr(unpivot_expr)

# Filter out rows where offer_value is null
unpivoted_df = unpivoted_df.filter(col("offer_value").isNotNull())

# Assign percentage based on the offer name
unpivoted_df = unpivoted_df.withColumn(
    "percentage",
    when(col("offer_name") == "offer_1", 5)
    .when(col("offer_name") == "offer_2", 10)
    .when(col("offer_name") == "offer_3", 15)
    .when(col("offer_name") == "offer_4", 20)
    .when(col("offer_name") == "offer_5", 25)
)

# Create offer_id by concatenating offer_name and percentage
unpivoted_df = unpivoted_df.withColumn(
    "offer_id",
    concat_ws("-",  col("percentage"),col("offer_name"))
)

# Select the required columns for offer_Dim DataFrame
offer_Dim = unpivoted_df.select(
    col('offer_id'),
    col('offer_name').alias('offer_name'),
    col('percentage')
).distinct()

##########################Creating Static date_Dim#########################

# Check if the retail_dwh path exists in HDFS to determine initial creation or not
hdfs_path = "/retail_dwh/date_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, date_format, dayofmonth, month, year, quarter, substring_index, split, when, concat_ws, lit,round
    from pyspark.sql.types import DateType
    from datetime import datetime, timedelta
    import os

    # Define the start and end dates
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2030, 12, 31)

    # Create a list of dates between start_date and end_date
    date_list = [(start_date + timedelta(days=x)) for x in range((end_date - start_date).days + 1)]

    # Convert the list of dates to a DataFrame
    date_df = spark.createDataFrame([(date,) for date in date_list], ["value"])

    # Add a date_id column formatted as 'yyyyMMdd'
    date_Dim = date_df.withColumn(
        "date_id",
        date_format(col("value"), "yyyyMMdd")
    )

    # Add day, month, year, quarter, and day_name columns
    date_Dim = date_Dim.withColumn("day", dayofmonth(col("value"))) \
                       .withColumn("month", month(col("value"))) \
                       .withColumn("year", year(col("value"))) \
                       .withColumn("quarter", quarter(col("value"))) \
                       .withColumn("day_name", date_format(col("value"), "EEEE"))

    # Change the data type of the value column to DateType
    date_Dim = date_Dim.withColumn("value", col("value").cast(DateType()))
    
    
else:
    print("Skipping the creation of the date dimension DataFrame.")




##########################Handle transaction Fact table#########################

#extract the needed columns
df = sales_transactions.drop("customer_fname", "customer_lname","customer_email","product_name","product_category")

#chenge transaction_date data type to date
df = df.withColumn("transaction_date", col("transaction_date").cast("date"))

#add transaction_date_id
df = df.withColumn(
    "transaction_date_id",
    date_format(df["transaction_date"], "yyyyMMdd")
)

#add location_id

split_col = split(sales_transactions['shipping_address'], '/')

df = df.withColumn('city', split_col.getItem(1)) \
                       .withColumn('state', split_col.getItem(2))

df = df.withColumn("location_id", concat_ws("-", col("city"), col("state")))

#add payment_id
df = df.withColumn("payment_id", concat_ws("-", col("is_online"), col("payment_method")))

#add offer_id

offer_columns = ['offer_1', 'offer_2', 'offer_3', 'offer_4', 'offer_5']

df = df.withColumn('offer_name',
                   concat_ws(',',
                             *[when(col(col_name), lit(col_name)).otherwise(None) for col_name in offer_columns]
                             )
                  )

df = df.withColumn(
    "percentage",
    when(col("offer_name") == "offer_1", 5)
    .when(col("offer_name") == "offer_2", 10)
    .when(col("offer_name") == "offer_3", 15)
    .when(col("offer_name") == "offer_4", 20)
    .when(col("offer_name") == "offer_5", 25)
    .otherwise(0) 
)


df = df.withColumn(
    "offer_id",
    when(col("percentage") != 0, concat_ws("-", col("percentage"), col("offer_name")))
    .otherwise(None)  
)

#add total_price
df = df.withColumn('total_price', round(col('units') * col('unit_price') * (1 - col('percentage') / 100), 3))

sales_transactions_Fact = df.select(
    col('transaction_id'),
    col('transaction_date_id'),
    col('customer_id'),
    col('sales_agent_id'),
    col('branch_id'),
    col('product_id'),
    col('location_id'),
    col('payment_id'),
    col('offer_id'),
    col('units'),
    col('unit_price'),
    col('total_price'))
    
##################Writing tables into hive###############

#Create retail database

spark.sql("CREATE DATABASE IF NOT EXISTS retail")

# Writing the sales_transactions_Fact into Hive

from pyspark.sql import functions as F

sales_transactions_Fact = sales_transactions_Fact.withColumn("year", F.substring("transaction_date_id", 1, 4))
sales_transactions_Fact = sales_transactions_Fact.withColumn("month", F.substring("transaction_date_id", 5, 2))
sales_transactions_Fact = sales_transactions_Fact.withColumn("day", F.substring("transaction_date_id", 7, 2))

# Check if the retail_dwh path exists in HDFS to determine initial or incremental loading
hdfs_path = "/retail_dwh/sales_transactions_Fact"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:

    # Writing the DataFrame with partitioning
    table_name = "retail.sales_transactions_Fact"
    sales_transactions_Fact.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .partitionBy("year", "month", "day") \
      .option("path", "/retail_dwh/sales_transactions_Fact") \
      .saveAsTable(table_name)
else:

    # Writing the DataFrame with partitioning
    table_name = "retail.sales_transactions_Fact"
    sales_transactions_Fact.coalesce(1).write.mode("append") \
      .format("parquet") \
      .partitionBy("year", "month", "day") \
      .option("path", "/retail_dwh/sales_transactions_Fact") \
      .saveAsTable(table_name)
    

# Writing the branches_Dim into Hive

# Check if the retail_dwh path exists in HDFS to determine initial or incremental loading

hdfs_path = "/retail_dwh/branches_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:

    table_name = "retail.branches_Dim"
    branches_Dim.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .option("path", "/retail_dwh/branches_Dim") \
      .saveAsTable(table_name)

else:
    #apply incremental Loading
    branches_Dim_i=spark.sql("select * from retail.branches_Dim")
    #Finding the new records only 
    branches_Dim_n = branches_Dim.join(branches_Dim_i, ['branch_id'], 'left_anti')
   
    table_name = "retail.branches_Dim"
    branches_Dim_n.coalesce(1).write.mode("append") \
      .format("parquet") \
      .option("path", "/retail_dwh/branches_Dim") \
      .saveAsTable(table_name)
    

# Writing the sales_agents_Dim into Hive

hdfs_path = "/retail_dwh/sales_agents_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:
    #apply incremental Loading
    table_name = "retail.sales_agents_Dim"
    sales_agents_Dim.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .option("path", "/retail_dwh/sales_agents_Dim") \
      .saveAsTable(table_name)
else:
    #apply incremental Loading
    sales_agents_Dim_i=spark.sql("select * from retail.sales_agents_Dim")
    #Finding the new records only 
    sales_agents_Dim_n = sales_agents_Dim.join(sales_agents_Dim_i,['sales_person_id'], 'left_anti')

    table_name = "retail.sales_agents_Dim"
    sales_agents_Dim_n.coalesce(1).write.mode("append") \
      .format("parquet") \
      .option("path", "/retail_dwh/sales_agents_Dim") \
      .saveAsTable(table_name)
  
# Writing the customer_Dim into Hive

hdfs_path = "/retail_dwh/customer_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:
    
    table_name = "retail.customer_Dim"
    customer_Dim.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .option("path", "/retail_dwh/customer_Dim") \
      .saveAsTable(table_name)
else:
    #apply incremental Loading
    customer_Dim_i=spark.sql("select * from retail.customer_Dim")
    #Finding the new records only 
    customer_Dim_n = customer_Dim.join(customer_Dim_i,['customer_id'], 'left_anti')
    
    table_name = "retail.customer_Dim"
    customer_Dim_n.coalesce(1).write.mode("append") \
      .format("parquet") \
      .option("path", "/retail_dwh/customer_Dim") \
      .saveAsTable(table_name)
  
# Writing the location_Dim into Hive

hdfs_path = "/retail_dwh/location_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:
    
    table_name = "retail.location_Dim"
    location_Dim.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .option("path", "/retail_dwh/location_Dim") \
      .saveAsTable(table_name)
else:
    #apply incremental Loading
    location_Dim_i=spark.sql("select * from retail.location_Dim")
    #Finding the new records only 
    location_Dim_n = location_Dim.join(location_Dim_i,['location_id'], 'left_anti')
    
    table_name = "retail.location_Dim"
    location_Dim_n.coalesce(1).write.mode("append") \
      .format("parquet") \
      .option("path", "/retail_dwh/location_Dim") \
      .saveAsTable(table_name)
  
# Writing the payment_Dim into Hive

hdfs_path = "/retail_dwh/payment_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:
    
    table_name = "retail.payment_Dim"
    payment_Dim.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .option("path", "/retail_dwh/payment_Dim") \
      .saveAsTable(table_name)
else:
    #apply incremental Loading
    payment_Dim_i=spark.sql("select * from retail.payment_Dim")
    #Finding the new records only 
    payment_Dim_n = payment_Dim.join(payment_Dim_i,['payment_id'], 'left_anti')
    
    table_name = "retail.payment_Dim"
    payment_Dim_n.coalesce(1).write.mode("append") \
      .format("parquet") \
      .option("path", "/retail_dwh/payment_Dim") \
      .saveAsTable(table_name)
  
# Writing the product_Dim into Hive

hdfs_path = "/retail_dwh/product_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:
    
    table_name = "retail.product_Dim"
    product_Dim.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .option("path", "/retail_dwh/product_Dim") \
      .saveAsTable(table_name)
else:
    product_Dim_i=spark.sql("select * from retail.product_Dim")
    #Finding the new records only 
    product_Dim_n = product_Dim.join(product_Dim_i,['product_id'], 'left_anti')
    
    table_name = "retail.product_Dim"
    product_Dim_n.coalesce(1).write.mode("append") \
      .format("parquet") \
      .option("path", "/retail_dwh/product_Dim") \
      .saveAsTable(table_name)
      
# Writing the offer_Dim into Hive

hdfs_path = "/retail_dwh/offer_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:
    
    table_name = "retail.offer_Dim"
    offer_Dim.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .option("path", "/retail_dwh/offer_Dim") \
      .saveAsTable(table_name)
else:
    offer_Dim_i=spark.sql("select * from retail.offer_Dim")
    #Finding the new records only 
    offer_Dim_n = offer_Dim.join(offer_Dim_i,['offer_id'], 'left_anti')
    
    table_name = "retail.offer_Dim"
    offer_Dim_n.coalesce(1).write.mode("append") \
      .format("parquet") \
      .option("path", "/retail_dwh/offer_Dim") \
      .saveAsTable(table_name)
  
# Writing the date_Dim into Hive

hdfs_path = "/retail_dwh/date_Dim"

if not os.system(f"hdfs dfs -test -d {hdfs_path}") == 0:

    table_name = "retail.date_Dim"
    date_Dim.coalesce(1).write.mode("overwrite") \
      .format("parquet") \
      .option("path", "/retail_dwh/date_Dim") \
      .saveAsTable(table_name)
else:
    print("Skipping the writing of the date dimension.")

# Stop the Spark session
spark.stop()
