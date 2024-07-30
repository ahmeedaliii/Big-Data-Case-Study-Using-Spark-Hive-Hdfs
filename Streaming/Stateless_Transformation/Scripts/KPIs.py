# Import necessary libraries
from pyspark.sql import SparkSession
# Initialize a Spark session
spark = SparkSession.builder \
    .appName("KPI Queries for Hive Tables") \
    .enableHiveSupport() \
    .getOrCreate()
	
	
	# 1. Product View Table (emp.product_view)
# KPI 1: Total Number of Product Views
total_product_views = spark.sql("""
        SELECT COUNT(*) AS total_product_views
        FROM stream.product_view
    """)
# Show the results
total_product_views.show()


# KPI 2: Number of Product Views by Category
views_per_category = spark.sql("""
    SELECT category, COUNT(*) AS views_per_category
    FROM stream.product_view
    GROUP BY category
    ORDER BY views_per_category DESC
""")
views_per_category.show()


# KPI 3: Unique Customers per Category
unique_customers_per_category = spark.sql("""
    SELECT category, COUNT(DISTINCT customerId) AS unique_customers
    FROM stream.product_view
    GROUP BY category
    ORDER BY unique_customers DESC
""")
unique_customers_per_category.show()


# 2. Add to Cart Table (emp.add_to_cart)
# KPI 1: Total Number of Add to Cart Actions
total_add_to_cart = spark.sql("""
    SELECT COUNT(*) AS total_add_to_cart
    FROM stream.add_to_cart
""")
total_add_to_cart.show()



# KPI 2: Number of Add to Cart Actions by Product
add_to_cart_per_product = spark.sql("""
    SELECT productId, COUNT(*) AS add_to_cart_per_product
    FROM stream.add_to_cart
    GROUP BY productId
    ORDER BY add_to_cart_per_product DESC
""")
add_to_cart_per_product.show()



# KPI 3: Total Quantity Added to Cart by Customer
total_quantity_added_by_customer = spark.sql("""
    SELECT customerId, SUM(quantity) AS total_quantity_added
    FROM stream.add_to_cart
    GROUP BY customerId
    ORDER BY total_quantity_added DESC
""")
total_quantity_added_by_customer.show()




# 3. Purchase Table (emp.purchase)

# KPI 1: Total Revenue Generated
total_revenue = spark.sql("""
    SELECT SUM(totalAmount ) AS total_revenue
    FROM stream.purchase
""")
total_revenue.show()




# KPI 2: Number of Purchases by Payment Method
purchases_per_method = spark.sql("""
    SELECT paymentMethod, COUNT(*) AS purchases_per_method
    FROM stream.purchase
    GROUP BY paymentMethod
    ORDER BY purchases_per_method DESC
""")
purchases_per_method.show()



# KPI 3: Average Order Value
average_order_value = spark.sql("""
    SELECT AVG(totalAmount) AS average_order_value
    FROM stream.purchase
""")
average_order_value.show()



# 4. Recommendation Click Table (emp.recommendation_click)

# KPI 1: Total Number of Recommendation Clicks
total_recommendation_clicks = spark.sql("""
    SELECT COUNT(*) AS total_recommendation_clicks
    FROM stream.recommendation_click
""")
total_recommendation_clicks.show()




# KPI 2: Number of Clicks by Recommendation Algorithm
clicks_per_algorithm = spark.sql("""
    SELECT algorithm, COUNT(*) AS clicks_per_algorithm
    FROM stream.recommendation_click
    GROUP BY algorithm
    ORDER BY clicks_per_algorithm DESC
""")
clicks_per_algorithm.show()




# KPI 3: Unique Customers Clicking on Recommendations
unique_customers_clicking = spark.sql("""
    SELECT COUNT(DISTINCT customerId) AS unique_customers_clicking
    FROM stream.recommendation_click
""")
unique_customers_clicking.show()
