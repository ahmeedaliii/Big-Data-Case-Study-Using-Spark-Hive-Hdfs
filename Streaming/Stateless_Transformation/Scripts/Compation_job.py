from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HDFSCompactionJob") \
    .getOrCreate()
# Import Hadoop FileSystem API
java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
java_import(spark._jvm, "org.apache.hadoop.fs.Path")
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.FileSystem.get(hadoop_conf)

def compact_files(directory_path, output_path):
    print(f"Compacting files in directory: {directory_path}")

    try:
        # Read all Parquet files from the directory
        df = spark.read.parquet(directory_path)

        # Coalesce for single output file
        compacted_data = df.coalesce(1)

        # Write the compacted file to a temporary location
        temp_output_path = output_path 
        compacted_data.write.mode("overwrite").parquet(temp_output_path)

        print(f"Compacted data written to: {temp_output_path}")

        # Delete original files
        for file_status in fs.listStatus(spark._jvm.Path(directory_path)):
            fs.delete(file_status.getPath(), True)  # recursively delete
            
        # Move compacted file to original directory using FileSystem API
        temp_files = fs.globStatus(spark._jvm.Path(f"{temp_output_path}/part-00000*.parquet"))
        for file in temp_files:
            fs.rename(file.getPath(), spark._jvm.Path(directory_path + "/" + file.getPath().getName()))

        print(f"Moved compacted file to original directory: {directory_path}")

        # Clean up temporary output directory
        fs.delete(spark._jvm.Path(temp_output_path), True)
        print(f"Deleted temporary output directory: {temp_output_path}")

    except Exception as e:
        print(f"Error during compaction for directory {directory_path}: {str(e)}")

    finally:
        # Optionally, delete the compacted directory after completion
        try:
            fs.delete(spark._jvm.Path(output_path), True)
            print(f"Deleted compacted directory: {output_path}")
        except Exception as e:
            print(f"Error deleting compacted directory {output_path}: {str(e)}")

# Define HDFS paths
hdfs_path_product_view = "/stream/product_view"
output_path_product_view = "/stream/product_viewcompacted"
hdfs_path_add_to_cart = "/stream/add_to_cart"
output_path_add_to_cart= "/stream/add_to_cartcompacted"
hdfs_path_purchase = "/stream/purchase"
output_path_purchase = "/stream/purchasecompacted"
hdfs_path_recommendation_click = "/stream/recommendation_click"
output_path_recommendation_click = "/stream/recommendation_clickcompacted"

# Compact the files
compact_files(hdfs_path_product_view, output_path_product_view)
compact_files(hdfs_path_add_to_cart, output_path_add_to_cart)
compact_files(hdfs_path_purchase, output_path_purchase)
compact_files(hdfs_path_recommendation_click, output_path_recommendation_click)
# Stop the Spark session
spark.stop()
