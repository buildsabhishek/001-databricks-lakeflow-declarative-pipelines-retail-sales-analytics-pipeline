from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ---------------------------------------------------------
# HELPER: Date parsing
# Supports ISO, DD-MM-YYYY and slash-separated dates
# ---------------------------------------------------------

def parse_date(column_name):
    c = F.trim(F.col(column_name))

    return F.coalesce(
        F.try_to_date(c, "yyyy-MM-dd"),
        F.try_to_date(c, "dd-MM-yyyy"),
        F.try_to_date(c, "d-M-yyyy"),
        F.try_to_date(c, "dd/MM/yyyy"),
        F.try_to_date(c, "d/M/yyyy"),
        F.try_to_date(c, "MM/dd/yyyy"),
        F.try_to_date(c, "M/d/yyyy"),
        F.try_to_timestamp(c).cast("date")
    )


# ---------------------------------------------------------
# SILVER: Customers
# ---------------------------------------------------------

@dp.materialized_view(
    name="nw_sales_dev.02_silver.customers",
    comment="Cleaned customer dimension"
)

@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL AND customer_id <> ''"
)
@dp.expect_or_drop(
    "valid_company_name",
    "company_name IS NOT NULL AND company_name <> ''"
)

def silver_customers():

    return (
        spark.read.table("nw_sales_dev.01_bronze.customers")
        .select(
            F.trim("CustomerID").alias("customer_id"),
            F.trim("CompanyName").alias("company_name"),
            F.trim("Country").alias("country"),
            F.trim("Region").alias("region"),
            F.trim("City").alias("city")
        )
        .dropDuplicates()
    )


# ---------------------------------------------------------
# SILVER: Categories
# ---------------------------------------------------------

@dp.materialized_view(
    name="nw_sales_dev.02_silver.categories",
    comment="Cleaned product categories"
)

@dp.expect_or_drop(
    "valid_category_id",
    "category_id IS NOT NULL AND category_id > 0"
)

def silver_categories():

    return (
        spark.read.table("nw_sales_dev.01_bronze.categories")
        .select(
            F.expr("try_cast(CategoryID AS INT)").alias("category_id"),
            F.trim("CategoryName").alias("category_name")
        )
        .dropDuplicates()
    )

# ---------------------------------------------------------
# SILVER: Products
# ---------------------------------------------------------

@dp.materialized_view(
    name="nw_sales_dev.02_silver.products",
    comment="Cleaned product dimension"
)

@dp.expect_or_drop(
    "valid_product_id",
    "product_id IS NOT NULL AND product_id > 0"
)
@dp.expect_or_drop(
    "valid_unit_price",
    "unit_price IS NOT NULL AND unit_price >= 0"
)

def silver_products():

    return (
        spark.read.table("nw_sales_dev.01_bronze.products")
        .select(
            F.expr("try_cast(ProductID AS INT)").alias("product_id"),
            F.trim("ProductName").alias("product_name"),
            F.expr("try_cast(CategoryID AS INT)").alias("category_id"),
            F.expr("try_cast(UnitPrice AS DECIMAL(18,2))").alias("unit_price"),
            F.expr("try_cast(UnitsInStock AS INT)").alias("units_in_stock"),
            F.expr("try_cast(Discontinued AS INT)").alias("discontinued")
        )
        .dropDuplicates()
    )

# ---------------------------------------------------------
# SILVER: Orders
# ---------------------------------------------------------

@dp.table(
    name="nw_sales_dev.02_silver.orders",
    comment="Cleaned sales orders"
)

@dp.expect_or_drop(
    "valid_order_id",
    "order_id IS NOT NULL AND order_id > 0"
)
@dp.expect(
    "valid_order_date",
    "order_date IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_freight",
    "freight IS NULL OR freight >= 0")

def silver_orders():

    return (
        spark.read.table("nw_sales_dev.01_bronze.orders")
        .select(
            F.expr("try_cast(OrderID AS INT)").alias("order_id"),
            F.trim("CustomerID").alias("customer_id"),

            parse_date("OrderDate").alias("order_date"),
            parse_date("RequiredDate").alias("required_date"),
            parse_date("ShippedDate").alias("shipped_date"),

            F.expr("try_cast(Freight AS DECIMAL(18,2))").alias("freight")
        )
        .dropDuplicates()
    )

# ---------------------------------------------------------
# SILVER: Order Details
# ---------------------------------------------------------

@dp.table(
    name="nw_sales_dev.02_silver.order_details",
    comment="Cleaned order line items"
)

@dp.expect_or_drop(
    "valid_order_and_product",
    "order_id IS NOT NULL AND product_id IS NOT NULL "
    "AND order_id > 0 AND product_id > 0"
)
@dp.expect_or_drop(
    "valid_quantity",
    "quantity IS NOT NULL AND quantity > 0"
)
@dp.expect_or_drop(
    "valid_discount",
    "discount IS NOT NULL AND discount BETWEEN 0 AND 1"
)
@dp.expect_or_drop(
    "valid_unit_price",
    "unit_price IS NOT NULL AND unit_price >= 0"
)

def silver_order_details():

    return (
        spark.read.table("nw_sales_dev.01_bronze.order_details")
        .select(
            F.expr("try_cast(OrderID AS INT)").alias("order_id"),
            F.expr("try_cast(ProductID AS INT)").alias("product_id"),
            F.expr("try_cast(UnitPrice AS DECIMAL(18,2))").alias("unit_price"),
            F.expr("try_cast(Quantity AS INT)").alias("quantity"),
            F.expr("try_cast(Discount AS DECIMAL(9,4))").alias("discount")
        )
        .dropDuplicates()
    )
