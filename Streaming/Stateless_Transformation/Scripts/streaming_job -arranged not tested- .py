from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, MapType
from py4j.java_gateway import java_import

spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Kafka connection details
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_topic = "sami_topic"  # Add your topic name here
kafka_username = "JUKQQM4ZM632RECA"
kafka_password = "UUkrPuSttgOC0U9lY3ZansNsKfN9fbxZPFwrGxudDrfv+knTD4rCwK+KdIzVPX0D"

# Import Hadoop FileSystem API
java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
java_import(spark._jvm, "org.apache.hadoop.fs.Path")
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.FileSystem.get(hadoop_conf)

def create_directories(paths):
    for path in paths:
        hdfs_path = spark._jvm.Path(path)
        if not fs.exists(hdfs_path):
            fs.mkdirs(hdfs_path)

# Define common schema for JSON parsing
schema = StructType([
    StructField("eventType", StringType(), True),
    StructField("customerId", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True),
    StructField("quantity", IntegerType(), True),
    StructField("totalAmount", DoubleType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("recommendedProductId", StringType(), True),
    StructField("algorithm", StringType(), True)
])

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

# Parse the JSON data and extract fields
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# Filter and process events by type with specific schemas
product_view_df = json_df.filter(col("eventType") == "productView") \
                         .select("eventType", "customerId", "productId", "timestamp", "metadata.category", "metadata.source")

add_to_cart_df = json_df.filter(col("eventType") == "addToCart") \
                        .select("eventType", "customerId", "productId", "timestamp", "quantity")

purchase_df = json_df.filter(col("eventType") == "purchase") \
                     .select("eventType", "customerId", "productId", "timestamp", "quantity", "totalAmount", "paymentMethod")

recommendation_click_df = json_df.filter(col("eventType") == "recommendationClick") \
                                 .select("eventType", "customerId", "recommendedProductId", "algorithm", "timestamp")

# Define the HDFS paths
hdfs_paths = {
    "product_view": "/stream/product_view",
    "add_to_cart": "/stream/add_to_cart",
    "purchase": "/stream/purchase",
    "recommendation_click": "/stream/recommendation_click"
}
checkpoint_paths = {
    "product_view": "/stream/product_viewcheck",
    "add_to_cart": "/stream/add_to_cartcheck",
    "purchase": "/stream/purchasecheck",
    "recommendation_click": "/stream/recommendation_clickcheck"
}

create_directories(list(hdfs_paths.values()) + list(checkpoint_paths.values()))

json_df.printSchema()
recommendation_click_df.printSchema()

# Write the data to HDFS as Parquet files with trigger time configuration
def write_stream(df, path, checkpoint_path):
    return df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime='10 seconds') \
        .start()

product_view_query = write_stream(product_view_df, hdfs_paths["product_view"], checkpoint_paths["product_view"])
add_to_cart_query = write_stream(add_to_cart_df, hdfs_paths["add_to_cart"], checkpoint_paths["add_to_cart"])
purchase_query = write_stream(purchase_df, hdfs_paths["purchase"], checkpoint_paths["purchase"])
recommendation_click_query = write_stream(recommendation_click_df, hdfs_paths["recommendation_click"], checkpoint_paths["recommendation_click"])

# Define the external tables using Spark SQL
spark.sql("CREATE DATABASE IF NOT EXISTS stream")

external_table_queries = [
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS stream.product_view (
        eventType STRING,
        customerId STRING,
        productId STRING,
        timestamp STRING,
        category STRING,
        source STRING
    )
    STORED AS PARQUET
    LOCATION '{hdfs_paths["product_view"]}'
    """,
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS stream.add_to_cart (
        eventType STRING,
        customerId STRING,
        productId STRING,
        timestamp STRING,
        quantity INT
    )
    STORED AS PARQUET
    LOCATION '{hdfs_paths["add_to_cart"]}'
    """,
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS stream.purchase (
        eventType STRING,
        customerId STRING,
        productId STRING,
        timestamp STRING,
        quantity INT,
        totalAmount DOUBLE,
        paymentMethod STRING
    )
    STORED AS PARQUET
    LOCATION '{hdfs_paths["purchase"]}'
    """,
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS stream.recommendation_click (
        eventType STRING,
        customerId STRING,
        recommendedProductId STRING,
        algorithm STRING,
        timestamp STRING
    )
    STORED AS PARQUET
    LOCATION '{hdfs_paths["recommendation_click"]}'
    """
]

for query in external_table_queries:
    spark.sql(query)

# Await termination of the queries
product_view_query.awaitTermination()
add_to_cart_query.awaitTermination()
purchase_query.awaitTermination()
recommendation_click_query.awaitTermination()
