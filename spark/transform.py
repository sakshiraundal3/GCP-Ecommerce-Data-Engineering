from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


spark = SparkSession.builder \
    .appName("EcommerceTransformation") \
    .getOrCreate()


# Bronze input paths
customers_path = "gs://ecommerce-data-sakshi-2026-01/bronze/customers/customers.csv"
products_path = "gs://ecommerce-data-sakshi-2026-01/bronze/products/products.csv"
orders_path = "gs://ecommerce-data-sakshi-2026-01/bronze/orders/orders.csv"
payments_path = "gs://ecommerce-data-sakshi-2026-01/bronze/payments/payments.csv"


# Read Bronze data
customers = spark.read.option("header", "true").option("inferSchema", "true").csv(customers_path)
products = spark.read.option("header", "true").option("inferSchema", "true").csv(products_path)
orders = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_path)
payments = spark.read.option("header", "true").option("inferSchema", "true").csv(payments_path)


# Clean customers
customers_clean = customers \
    .dropDuplicates(["customer_id"]) \
    .withColumn("signup_date", to_date(col("signup_date")))


# Clean products
products_clean = products \
    .dropDuplicates(["product_id"]) \
    .withColumn("price", col("price").cast("double"))


# Clean orders
orders_clean = orders \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("quantity", col("quantity").cast("integer")) \
    .withColumn("amount", col("amount").cast("double"))


# Clean payments
payments_clean = payments \
    .dropDuplicates(["payment_id"]) \
    .withColumn("payment_date", to_date(col("payment_date"))) \
    .withColumn("amount", col("amount").cast("double"))


# Write Silver data as Parquet
customers_clean.write.mode("overwrite").parquet(
    "gs://ecommerce-data-sakshi-2026-01/silver/customers"
)

products_clean.write.mode("overwrite").parquet(
    "gs://ecommerce-data-sakshi-2026-01/silver/products"
)

orders_clean.write.mode("overwrite").parquet(
    "gs://ecommerce-data-sakshi-2026-01/silver/orders"
)

payments_clean.write.mode("overwrite").parquet(
    "gs://ecommerce-data-sakshi-2026-01/silver/payments"
)


print("E-commerce Bronze to Silver transformation completed successfully!")

spark.stop()