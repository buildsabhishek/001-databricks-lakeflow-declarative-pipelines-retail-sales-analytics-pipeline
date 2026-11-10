from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# ---------------------------------------------------------
# Source locations
# ---------------------------------------------------------

ORDERS_PATH = (
    "/Volumes/nw_sales_dev/00_landing/nw_sales/orders/"
)

ORDER_DETAILS_PATH = (
    "/Volumes/nw_sales_dev/00_landing/nw_sales/orderdetails/"
)


# ---------------------------------------------------------
# Explicit schemas: all source fields are STRING
# ---------------------------------------------------------

orders_schema = StructType([
    StructField(c, StringType(), True)
    for c in [
        "OrderID", "CustomerID", "EmployeeID",
        "OrderDate", "RequiredDate", "ShippedDate",
        "ShipVia", "Freight",
        "ShipPostalCode"
    ]
])

order_details_schema = StructType([
    StructField(c, StringType(), True)
    for c in [
        "OrderID", "ProductID", "UnitPrice",
        "Quantity", "Discount"
    ]
])

# ---------------------------------------------------------
# BRONZE: Orders
# ---------------------------------------------------------

@dp.table(
    name="nw_sales_dev.01_bronze.orders",
    comment="Raw orders ingested incrementally from CSV",
    table_properties={"quality": "bronze"}
)
def bronze_orders():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(orders_schema)
        .load(ORDERS_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ---------------------------------------------------------
# BRONZE: Order Details
# ---------------------------------------------------------

@dp.table(
    name="nw_sales_dev.01_bronze.order_details",
    comment="Raw order details ingested incrementally from CSV",
    table_properties={"quality": "bronze"}
)
def bronze_order_details():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(order_details_schema)
        .load(ORDER_DETAILS_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
