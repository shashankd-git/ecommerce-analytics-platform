# ============================================
# Olist Dataset Schema Definitions
# Bronze Layer — Schema Enforcement
# Project: E-Commerce Analytics Platform
# ============================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType
)

# ============================================
# Orders Schema
# Core table — joins to everything
# ============================================
orders_schema = StructType([
    StructField("order_id",
        StringType(), False),
    StructField("customer_id",
        StringType(), False),
    StructField("order_status",
        StringType(), True),
    StructField("order_purchase_timestamp",
        StringType(), True),
    StructField("order_approved_at",
        StringType(), True),
    StructField("order_delivered_carrier_date",
        StringType(), True),
    StructField("order_delivered_customer_date",
        StringType(), True),
    StructField("order_estimated_delivery_date",
        StringType(), True)
])

# ============================================
# Order Items Schema
# One row per item per order
# ============================================
order_items_schema = StructType([
    StructField("order_id",
        StringType(), False),
    StructField("order_item_id",
        IntegerType(), False),
    StructField("product_id",
        StringType(), False),
    StructField("seller_id",
        StringType(), False),
    StructField("shipping_limit_date",
        StringType(), True),
    StructField("price",
        DoubleType(), True),
    StructField("freight_value",
        DoubleType(), True)
])

# ============================================
# Customers Schema
# ============================================
customers_schema = StructType([
    StructField("customer_id",
        StringType(), False),
    StructField("customer_unique_id",
        StringType(), True),
    StructField("customer_zip_code_prefix",
        StringType(), True),
    StructField("customer_city",
        StringType(), True),
    StructField("customer_state",
        StringType(), True)
])

# ============================================
# Products Schema
# ============================================
products_schema = StructType([
    StructField("product_id",
        StringType(), False),
    StructField("product_category_name",
        StringType(), True),
    StructField("product_name_lenght",
        IntegerType(), True),
    StructField("product_description_lenght",
        IntegerType(), True),
    StructField("product_photos_qty",
        IntegerType(), True),
    StructField("product_weight_g",
        DoubleType(), True),
    StructField("product_length_cm",
        DoubleType(), True),
    StructField("product_height_cm",
        DoubleType(), True),
    StructField("product_width_cm",
        DoubleType(), True)
])

# ============================================
# Sellers Schema
# ============================================
sellers_schema = StructType([
    StructField("seller_id",
        StringType(), False),
    StructField("seller_zip_code_prefix",
        StringType(), True),
    StructField("seller_city",
        StringType(), True),
    StructField("seller_state",
        StringType(), True)
])

# ============================================
# Order Payments Schema
# ============================================
order_payments_schema = StructType([
    StructField("order_id",
        StringType(), False),
    StructField("payment_sequential",
        IntegerType(), True),
    StructField("payment_type",
        StringType(), True),
    StructField("payment_installments",
        IntegerType(), True),
    StructField("payment_value",
        DoubleType(), True)
])

# ============================================
# Order Reviews Schema
# Contains unstructured text (review_body)
# ============================================
order_reviews_schema = StructType([
    StructField("review_id",
        StringType(), False),
    StructField("order_id",
        StringType(), False),
    StructField("review_score",
        IntegerType(), True),
    StructField("review_comment_title",
        StringType(), True),
    StructField("review_comment_message",
        StringType(), True),
    StructField("review_creation_date",
        StringType(), True),
    StructField("review_answer_timestamp",
        StringType(), True)
])

# ============================================
# Geolocation Schema
# Largest table — 1M rows
# ============================================
geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix",
        StringType(), False),
    StructField("geolocation_lat",
        DoubleType(), True),
    StructField("geolocation_lng",
        DoubleType(), True),
    StructField("geolocation_city",
        StringType(), True),
    StructField("geolocation_state",
        StringType(), True)
])

# ============================================
# Category Name Translation Schema
# ============================================
category_translation_schema = StructType([
    StructField("product_category_name",
        StringType(), False),
    StructField("product_category_name_english",
        StringType(), True)
])

# ============================================
# Master config — maps each table to:
# ├── schema definition
# ├── source CSV filename
# ├── primary key column
# └── target Delta table name
# ============================================
OLIST_TABLE_CONFIG = {
    "olist_orders": {
        "schema": orders_schema,
        "source_file": "olist_orders_dataset.csv",
        "primary_key": "order_id",
        "target_table": "bronze_orders"
    },
    "olist_order_items": {
        "schema": order_items_schema,
        "source_file": "olist_order_items_dataset.csv",
        "primary_key": "order_id",
        "target_table": "bronze_order_items"
    },
    "olist_customers": {
        "schema": customers_schema,
        "source_file": "olist_customers_dataset.csv",
        "primary_key": "customer_id",
        "target_table": "bronze_customers"
    },
    "olist_products": {
        "schema": products_schema,
        "source_file": "olist_products_dataset.csv",
        "primary_key": "product_id",
        "target_table": "bronze_products"
    },
    "olist_sellers": {
        "schema": sellers_schema,
        "source_file": "olist_sellers_dataset.csv",
        "primary_key": "seller_id",
        "target_table": "bronze_sellers"
    },
    "olist_order_payments": {
        "schema": order_payments_schema,
        "source_file": "olist_order_payments_dataset.csv",
        "primary_key": "order_id",
        "target_table": "bronze_order_payments"
    },
    "olist_order_reviews": {
        "schema": order_reviews_schema,
        "source_file": "olist_order_reviews_dataset.csv",
        "primary_key": "review_id",
        "target_table": "bronze_order_reviews"
    },
    "olist_geolocation": {
        "schema": geolocation_schema,
        "source_file": "olist_geolocation_dataset.csv",
        "primary_key": "geolocation_zip_code_prefix",
        "target_table": "bronze_geolocation"
    },
    "olist_category_translation": {
        "schema": category_translation_schema,
        "source_file": "product_category_name_translation.csv",
        "primary_key": "product_category_name",
        "target_table": "bronze_category_translation"
    }
}