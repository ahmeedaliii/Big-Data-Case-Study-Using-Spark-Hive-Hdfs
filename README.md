# Big-Data-Case-Study-Using-Spark
 
## Overview
Q Company, specializing in retail, operates branches across various regions and utilizes an e-commerce platform. The company deals with a dynamic dataset including new products, customers, branches, and salespeople on a daily basis. The company also offers multiple customer discounts with specific redemption rules.

## Data Nature
The source system pushes three types of files every hour:
- Branches file
- Sales agents file
- Sales transactions file (including both branches and online sales)

Each file may contain new entries at any time. The schema for shipping addresses in online sales transactions includes:
- Address
- City
- State
- Postal code

The company application pushes logs to a Kafka cluster for later processing.

## Project Structure
The project is divided into batch and streaming parts, each with distinct technical and business requirements aimed at achieving comprehensive data processing and insightful reporting for Q Company.

![image](https://github.com/AliMagdy100/Big-Data-Case-Study-Using-Spark/assets/87953057/6469d365-6b9d-4687-97d3-2f9c5d57eea2)

## Batch Processing

### Technical Description
The batch processing part of the project involves the following steps:

1. **File Ingestion:**
   - Simulate the arrival of one group of files every hour by placing them in the local file system (LFS) manually or via automation.
   - Ingest these files into the data lake as raw files using Python.

2. **Data Storage:**
   - Store data in the data lake with appropriate partitioning to track the files.

3. **Data Processing:**
   - Process raw files to meet business requirements and store them in the data lake as Hive tables to serve as a Data Warehouse (DWH) using Spark.
   - Ensure data cleanliness.

### Business Requirements
- Design and implement a DWH model based on business needs.
- Focus on transaction date conditions for most queries.
- Add a column for total paid price after discount in the fact table.
- Provide insights for the marketing team, including:
  - Most selling products
  - Most redeemed offers by customers
  - Most redeemed offers per product
  - Lowest cities in online sales for targeted marketing campaigns
- Generate a daily CSV dump for the B2B team with the following columns:
  - Sales agent name
  - Product name
  - Total sold units
- Ensure the daily CSV is sent to the local file system.

## Streaming Processing

### Technical Description
The streaming processing part of the project involves the following steps:

1. **Kafka Producer:**
   - Utilize a Kafka Python producer to send application logs to a Kafka cluster. The logs have a dynamic schema, requiring analysis of the producer code to detect all possible data elements.

   - Alternatively, run the producer from a Jupyter notebook.

2. **Spark Streaming:**
   - Create a Spark streaming job to receive data from Kafka, process it, and store the processed data on HDFS.

### Business Requirements
- Design the data storage and processing to maximize value for business teams.
- Develop at least two queries or reports from the streaming data.

## Tools and Technologies
- **Mandatory:** Apache Spark, Apache Hive, Hadoop, Python, Cron


