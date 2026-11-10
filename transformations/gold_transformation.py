from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ---------------------------------------------------------
# GOLD: Sales Fact
# Grain: One row per OrderID + ProductID
# ---------------------------------------------------------

@dp.materialized_view(
    name="nw_sales_dev.03_gold.fact_sales",
    comment="Sales fact with customer, product and revenue metrics",
    table_properties={"quality": "gold"}
)
def gold_fact_sales():

    orders = spark.read.table(
        "nw_sales_dev.02_silver.orders"
    )

    details = spark.read.table(
        "nw_sales_dev.02_silver.order_details"
    )

    customers = spark.read.table(
        "nw_sales_dev.02_silver.customers"
    )

    products = spark.read.table(
        "nw_sales_dev.02_silver.products"
    )

    categories = spark.read.table(
        "nw_sales_dev.02_silver.categories"
    )

    df = (
        details.alias("d")
        .join(
            orders.alias("o"),
            F.col("d.order_id") == F.col("o.order_id"),
            "inner"
        )
        .join(
            products.alias("p"),
            F.col("d.product_id") == F.col("p.product_id"),
            "left"
        )
        .join(
            categories.alias("cat"),
            F.col("p.category_id") == F.col("cat.category_id"),
            "left"
        )
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left"
        )
        .select(
            F.col("d.order_id"),
            F.col("d.product_id"),
            F.col("o.customer_id"),
            F.col("o.order_date"),

            F.col("c.company_name"),
            F.col("c.country").alias("customer_country"),

            F.col("p.product_name"),
            F.col("cat.category_name"),

            F.col("d.quantity"),
            F.col("d.unit_price"),
            F.col("d.discount")
        )
    )

    return (
        df
        .withColumn(
            "gross_sales",
            (F.col("quantity") * F.col("unit_price"))
            .cast("decimal(18,2)")
        )
        .withColumn(
            "discount_amount",
            (
                F.col("gross_sales") * F.col("discount")
            ).cast("decimal(18,2)")
        )
        .withColumn(
            "net_sales",
            (
                F.col("gross_sales") - F.col("discount_amount")
            ).cast("decimal(18,2)")
        )
    )


# ---------------------------------------------------------
# GOLD: Daily Sales Performance
# Grain: One row per order date
# ---------------------------------------------------------

@dp.materialized_view(
    name="nw_sales_dev.03_gold.agg_daily_sales",
    comment="Daily sales KPIs",
    table_properties={"quality": "gold"}
)
def gold_daily_sales():

    df = spark.read.table(
        "nw_sales_dev.03_gold.fact_sales"
    )

    return (
        df.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("units_sold"),

            F.round(
                F.sum("gross_sales"), 2
            ).alias("gross_sales"),

            F.round(
                F.sum("discount_amount"), 2
            ).alias("total_discount"),

            F.round(
                F.sum("net_sales"), 2
            ).alias("net_sales")
        )
        .withColumn(
            "average_order_value",
            F.round(
                F.col("net_sales") / F.col("total_orders"),
                2
            )
        )
    )


# ---------------------------------------------------------
# GOLD: Product Performance
# Grain: One row per product
# ---------------------------------------------------------

@dp.materialized_view(
    name="nw_sales_dev.03_gold.agg_product_performance",
    comment="Product and category sales performance",
    table_properties={"quality": "gold"}
)
def gold_product_performance():

    df = spark.read.table(
        "nw_sales_dev.03_gold.fact_sales"
    )

    return (
        df.groupBy(
            "product_id",
            "product_name",
            "category_name"
        )
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("units_sold"),

            F.round(
                F.sum("gross_sales"), 2
            ).alias("gross_sales"),

            F.round(
                F.sum("net_sales"), 2
            ).alias("net_sales")
        )
    )


# ---------------------------------------------------------
# GOLD: Customer Performance
# Grain: One row per customer
# ---------------------------------------------------------

@dp.materialized_view(
    name="nw_sales_dev.03_gold.agg_customer_performance",
    comment="Customer purchasing and revenue performance",
    table_properties={"quality": "gold"}
)
def gold_customer_performance():

    df = spark.read.table(
        "nw_sales_dev.03_gold.fact_sales"
    )

    return (
        df.groupBy(
            "customer_id",
            "company_name",
            "customer_country"
        )
        .agg(
            F.countDistinct("order_id").alias("total_orders"),

            F.sum("quantity").alias("units_purchased"),

            F.round(
                F.sum("net_sales"), 2
            ).alias("net_sales"),

            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date")
        )
        .withColumn(
            "average_order_value",
            F.round(
                F.col("net_sales") / F.col("total_orders"),
                2
            )
        )
    )
