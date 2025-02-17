import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, MapType

# Function to create table if it doesn't exist
def create_table_if_not_exists():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS public.Top_Products (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        productId VARCHAR,
        count BIGINT
    );
    """
    connection = None
    try:
        connection = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="itversity",
            host="spark-sql-and-pyspark-using-python3-cluster_util_db-1",
            port="5432"
        )
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

# Run the function to create the table
create_table_if_not_exists()

# Initialize Spark session with necessary configurations
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("streamtodb2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .config("spark.jars", "/data/project/postgresql-42.7.0.jar") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.cores", "2") \
    .getOrCreate()

print("session opened")

# Define common schema for JSON parsing
schema = StructType([
    StructField("eventType", StringType(), True),
    StructField("customerId", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("timestamp", TimestampType(), True),  # Change type to TimestampType
    StructField("metadata", MapType(StringType(), StringType()), True),
    StructField("quantity", IntegerType(), True),
    StructField("totalAmount", FloatType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("recommendedProductId", StringType(), True),
    StructField("algorithm", StringType(), True)
])

print("schema read")

# Kafka connection details
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_topic = "sami_topic"  # Add your topic name here
kafka_username = "JUKQQM4ZM632RECA"
kafka_password = "UUkrPuSttgOC0U9lY3ZansNsKfN9fbxZPFwrGxudDrfv+knTD4rCwK+KdIzVPX0D"

# Read data from Kafka topic as a streaming DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .load()

# Parse JSON data and select relevant fields
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# Filter product views
product_views = parsed_df.filter(col("eventType") == "productView") \
                         .select("eventType", "productId", "timestamp")

# Calculate top 5 products in the last 30 minutes
top_products = product_views \
    .withWatermark("timestamp", "30 minute") \
    .groupBy(window(col("timestamp"), "30 minute"), col("productId")) \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(5)

print("will write to postgres now")

# Write aggregated results (top products) to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    postgres_url = "jdbc:postgresql://spark-sql-and-pyspark-using-python3-cluster_util_db-1:5432/postgres"
    postgres_properties = {
        "user": "postgres",
        "password": "itversity",
        "driver": "org.postgresql.Driver"
    }
    batch_df.selectExpr("window.start as window_start", "window.end as window_end", "productId", "count") \
        .write \
        .format("jdbc") \
        .mode("append") \
        .option("url", postgres_url) \
        .option("dbtable", "public.Top_Products") \
        .option("user", postgres_properties["user"]) \
        .option("password", postgres_properties["password"]) \
        .option("driver", postgres_properties["driver"]) \
        .save()

query = top_products.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("complete") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
