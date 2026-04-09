# ============================================
# Silver Layer Ingestion Script
# Project: E-Commerce Analytics Platform
# Description: Reads Bronze Delta tables,
#              cleans and standardizes data,
#              writes Silver Delta tables
#              using MERGE (upsert pattern)
# ============================================

# ============================================
# CONFIGURATION
# ============================================

STORAGE_ACCOUNT   = "ecommerceadls2026"
BRONZE_CONTAINER  = "bronze"
SILVER_CONTAINER  = "silver"
CLIENT_ID         = "YOUR_CLIENT_ID"
CLIENT_SECRET     = "YOUR_CLIENT_SECRET"
TENANT_ID         = "YOUR_TENANT_ID"

BRONZE_DELTA_PATH = (
    f"abfss://{BRONZE_CONTAINER}"
    f"@{STORAGE_ACCOUNT}"
    f".dfs.core.windows.net/_delta_tables/"
)
SILVER_DELTA_PATH = (
    f"abfss://{SILVER_CONTAINER}"
    f"@{STORAGE_ACCOUNT}"
    f".dfs.core.windows.net/_delta_tables/"
)

# ============================================
# IMPORTS
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower,
    when, lit, coalesce,
    to_timestamp, current_timestamp,
    current_date, round,
    unix_timestamp, datediff
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType
)
from delta.tables import DeltaTable
from datetime import datetime
import uuid

# ============================================
# AUTHENTICATION
# ============================================

def configure_auth(spark):
    """Configure Service Principal OAuth"""
    spark.conf.set(
        f"fs.azure.account.auth.type"
        f".{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type"
        f".{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs"
        ".oauth2.ClientCredsTokenProvider"
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id"
        f".{STORAGE_ACCOUNT}.dfs.core.windows.net",
        CLIENT_ID
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret"
        f".{STORAGE_ACCOUNT}.dfs.core.windows.net",
        CLIENT_SECRET
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint"
        f".{STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"https://login.microsoftonline.com"
        f"/{TENANT_ID}/oauth2/token"
    )
    print("✅ Authentication configured")

# ============================================
# SILVER HELPER FUNCTIONS
# ============================================

def add_silver_metadata(df):
    """Add Silver layer audit columns"""
    pipeline_run_id = str(uuid.uuid4())
    return df \
        .withColumn(
            "silver_updated_at",
            current_timestamp()
        ) \
        .withColumn(
            "silver_pipeline_run_id",
            lit(pipeline_run_id)
        )

def merge_to_silver(df, target_path, merge_key):
    """
    MERGE using single primary key.
    INSERT new records.
    UPDATE changed records.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        DeltaTable.forPath(spark, target_path) \
            .alias("target") \
            .merge(
                df.alias("source"),
                f"target.{merge_key} = "
                f"source.{merge_key}"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"✅ MERGE complete")
    else:
        df.write.format("delta") \
          .mode("overwrite").save(target_path)
        print(f"✅ Initial write complete")

def merge_composite(df, target_path, keys):
    """
    MERGE using composite key
    (multiple columns as primary key).
    Used for order_items and payments
    where order_id alone is not unique.
    """
    condition = " AND ".join([
        f"target.{k} = source.{k}"
        for k in keys
    ])
    if DeltaTable.isDeltaTable(spark, target_path):
        DeltaTable.forPath(spark, target_path) \
            .alias("target") \
            .merge(df.alias("source"), condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"✅ Composite MERGE complete")
    else:
        df.write.format("delta") \
          .mode("overwrite").save(target_path)
        print(f"✅ Initial write complete")

# ============================================
# TABLE TRANSFORMATIONS
# ============================================

def transform_orders(df):
    """
    Clean olist_orders:
    - Parse timestamps String → Timestamp
    - Standardize order_status to lowercase
    - Calculate approval_time_minutes
    - Calculate delivery_days
    - Flag is_late_delivery
    """
    return df \
        .dropDuplicates(["order_id"]) \
        .withColumn("order_purchase_timestamp",
            to_timestamp("order_purchase_timestamp",
                         "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_approved_at",
            to_timestamp("order_approved_at",
                         "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_delivered_carrier_date",
            to_timestamp("order_delivered_carrier_date",
                         "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_delivered_customer_date",
            to_timestamp("order_delivered_customer_date",
                         "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_estimated_delivery_date",
            to_timestamp("order_estimated_delivery_date",
                         "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_status",
            trim(lower(col("order_status")))) \
        .withColumn("approval_time_minutes",
            when(col("order_approved_at").isNotNull(),
                round(
                    (unix_timestamp("order_approved_at") -
                     unix_timestamp("order_purchase_timestamp"))
                    / 60, 2
                )
            ).otherwise(None)) \
        .withColumn("delivery_days",
            when(
                col("order_delivered_customer_date")
                .isNotNull(),
                datediff(
                    "order_delivered_customer_date",
                    "order_purchase_timestamp"
                )
            ).otherwise(None)) \
        .withColumn("is_late_delivery",
            when(
                col("order_delivered_customer_date")
                .isNotNull() &
                col("order_estimated_delivery_date")
                .isNotNull(),
                col("order_delivered_customer_date") >
                col("order_estimated_delivery_date")
            ).otherwise(None)) \
        .select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "approval_time_minutes",
            "delivery_days",
            "is_late_delivery"
        )


def transform_order_items(df):
    """
    Clean olist_order_items:
    - Validate price > 0
    - Validate freight_value >= 0
    - Calculate total_item_value
    - Parse shipping_limit_date
    NOTE: Uses composite key order_id + order_item_id
          because one order has multiple items
    """
    return df \
        .filter(col("price") > 0) \
        .filter(col("freight_value") >= 0) \
        .withColumn("total_item_value",
            round(
                col("price") + col("freight_value"), 2
            )) \
        .withColumn("shipping_limit_date",
            to_timestamp("shipping_limit_date",
                         "yyyy-MM-dd HH:mm:ss")) \
        .select(
            "order_id", "order_item_id",
            "product_id", "seller_id",
            "shipping_limit_date",
            "price", "freight_value",
            "total_item_value"
        )


def transform_customers(df):
    """
    Clean olist_customers:
    - Standardize city to lowercase
    - Standardize state to uppercase
    - Trim whitespace
    """
    return df \
        .dropDuplicates(["customer_id"]) \
        .withColumn("customer_city",
            trim(lower(col("customer_city")))) \
        .withColumn("customer_state",
            trim(upper(col("customer_state")))) \
        .withColumn("customer_zip_code_prefix",
            trim(col("customer_zip_code_prefix"))) \
        .filter(col("customer_id").isNotNull()) \
        .select(
            "customer_id", "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city", "customer_state"
        )


def transform_products(df, category_df):
    """
    Clean olist_products:
    - Join English category names
    - Fill null categories with 'unknown'
    - Fix column name typos (lenght → length)
    - Validate dimensions
    """
    return df \
        .join(
            category_df.select(
                "product_category_name",
                "product_category_name_english"
            ),
            on="product_category_name",
            how="left"
        ) \
        .dropDuplicates(["product_id"]) \
        .withColumn("product_category_name",
            coalesce(
                col("product_category_name"),
                lit("unknown")
            )) \
        .withColumn("product_category_name_english",
            coalesce(
                col("product_category_name_english"),
                lit("unknown")
            )) \
        .withColumnRenamed(
            "product_name_lenght",
            "product_name_length"
        ) \
        .withColumnRenamed(
            "product_description_lenght",
            "product_description_length"
        ) \
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        )


def transform_sellers(df):
    """
    Clean olist_sellers:
    - Standardize city to lowercase
    - Standardize state to uppercase
    """
    return df \
        .dropDuplicates(["seller_id"]) \
        .withColumn("seller_city",
            trim(lower(col("seller_city")))) \
        .withColumn("seller_state",
            trim(upper(col("seller_state")))) \
        .filter(col("seller_id").isNotNull()) \
        .select(
            "seller_id", "seller_zip_code_prefix",
            "seller_city", "seller_state"
        )


def transform_payments(df):
    """
    Clean olist_order_payments:
    - Remove zero/negative payment values
    - Standardize payment_type to lowercase
    - Round payment_value to 2 decimal places
    NOTE: Uses composite key order_id +
          payment_sequential because one order
          can have multiple payment methods
    """
    return df \
        .filter(col("payment_value") > 0) \
        .withColumn("payment_type",
            trim(lower(col("payment_type")))) \
        .withColumn("payment_value",
            round(col("payment_value"), 2)) \
        .select(
            "order_id", "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        )


def transform_reviews(df):
    """
    Clean olist_order_reviews:
    - Deduplicate by review_id
    - Validate review_score between 1-5
    - Parse timestamps safely
      (check starts with "20" to avoid
       parsing review text as timestamp)
    - Add has_comment flag
    NOTE: review_comment_message kept as-is
          Sentiment analysis done in Gold layer
    """
    return df \
        .dropDuplicates(["review_id"]) \
        .filter(col("review_score").between(1, 5)) \
        .withColumn("review_creation_date",
            when(
                col("review_creation_date")
                .isNotNull() &
                (col("review_creation_date") != "") &
                col("review_creation_date")
                .startswith("20"),
                to_timestamp(
                    col("review_creation_date"),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ).otherwise(None)
        ) \
        .withColumn("review_answer_timestamp",
            when(
                col("review_answer_timestamp")
                .isNotNull() &
                (col("review_answer_timestamp") != "") &
                col("review_answer_timestamp")
                .startswith("20"),
                to_timestamp(
                    col("review_answer_timestamp"),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ).otherwise(None)
        ) \
        .withColumn("has_comment",
            col("review_comment_message").isNotNull()) \
        .select(
            "review_id", "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
            "has_comment"
        )


def transform_geolocation(df):
    """
    Clean olist_geolocation:
    - Filter invalid Brazilian coordinates
      (lat: -34 to 6, lng: -74 to -32)
    - Standardize city to lowercase
    - Standardize state to uppercase
    - Deduplicate by zip code
      (keep one coordinate per zip code)
    NOTE: Full refresh pattern — static lookup
    """
    return df \
        .filter(
            col("geolocation_lat").between(-34, 6)
        ) \
        .filter(
            col("geolocation_lng").between(-74, -32)
        ) \
        .withColumn("geolocation_city",
            trim(lower(col("geolocation_city")))) \
        .withColumn("geolocation_state",
            trim(upper(col("geolocation_state")))) \
        .dropDuplicates(
            ["geolocation_zip_code_prefix"]
        ) \
        .select(
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        )


def transform_category_translation(df):
    """
    Clean category_translation:
    - Remove nulls
    - Standardize to lowercase
    NOTE: Full refresh — only 71 rows, static
    """
    return df \
        .dropDuplicates(["product_category_name"]) \
        .filter(
            col("product_category_name").isNotNull()
        ) \
        .withColumn("product_category_name",
            trim(lower(
                col("product_category_name")
            ))) \
        .withColumn("product_category_name_english",
            trim(lower(
                col("product_category_name_english")
            ))) \
        .select(
            "product_category_name",
            "product_category_name_english"
        )


# ============================================
# MAIN PIPELINE
# ============================================

def run_silver_pipeline(spark):
    """
    Main Silver pipeline:
    1. Read all 9 Bronze Delta tables
    2. Apply transformations per table
    3. Write Silver Delta tables using
       MERGE, composite MERGE or full refresh
    4. Optimize all Delta tables
    5. Print summary report
    """

    pipeline_run_id = str(uuid.uuid4())
    start_time      = datetime.now()
    results         = []

    print(f"\n{'='*60}")
    print(f"SILVER PIPELINE STARTED")
    print(f"Run ID:  {pipeline_run_id}")
    print(f"Started: {start_time}")
    print(f"{'='*60}\n")

    # ── Read Bronze tables ────────────────────
    print("Reading Bronze Delta tables...")

    bronze_orders = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_orders")
    bronze_items = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_order_items")
    bronze_customers = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_customers")
    bronze_products = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_products")
    bronze_sellers = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_sellers")
    bronze_payments = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_order_payments")
    bronze_reviews = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_order_reviews")
    bronze_geolocation = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_geolocation")
    bronze_categories = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH +
              "bronze_category_translation")

    print(f"✅ All Bronze tables loaded\n")

    # ── Transform all tables ──────────────────
    print("Applying transformations...")

    df_orders     = add_silver_metadata(
        transform_orders(bronze_orders))
    df_items      = add_silver_metadata(
        transform_order_items(bronze_items))
    df_customers  = add_silver_metadata(
        transform_customers(bronze_customers))
    df_products   = add_silver_metadata(
        transform_products(
            bronze_products, bronze_categories))
    df_sellers    = add_silver_metadata(
        transform_sellers(bronze_sellers))
    df_payments   = add_silver_metadata(
        transform_payments(bronze_payments))
    df_reviews    = add_silver_metadata(
        transform_reviews(bronze_reviews))
    df_geolocation = add_silver_metadata(
        transform_geolocation(bronze_geolocation))
    df_categories  = add_silver_metadata(
        transform_category_translation(
            bronze_categories))

    print(f"✅ All transformations applied\n")

    # ── Silver table write config ─────────────
    silver_tables = [
        {
            "name": "silver_orders",
            "df": df_orders,
            "keys": ["order_id"],
            "type": "merge"
        },
        {
            "name": "silver_order_items",
            "df": df_items,
            "keys": ["order_id", "order_item_id"],
            "type": "merge_composite"
        },
        {
            "name": "silver_customers",
            "df": df_customers,
            "keys": ["customer_id"],
            "type": "merge"
        },
        {
            "name": "silver_products",
            "df": df_products,
            "keys": ["product_id"],
            "type": "merge"
        },
        {
            "name": "silver_sellers",
            "df": df_sellers,
            "keys": ["seller_id"],
            "type": "merge"
        },
        {
            "name": "silver_payments",
            "df": df_payments,
            "keys": ["order_id", "payment_sequential"],
            "type": "merge_composite"
        },
        {
            "name": "silver_reviews",
            "df": df_reviews,
            "keys": ["review_id"],
            "type": "merge"
        },
        {
            "name": "silver_geolocation",
            "df": df_geolocation,
            "keys": ["geolocation_zip_code_prefix"],
            "type": "full_refresh"
        },
        {
            "name": "silver_categories",
            "df": df_categories,
            "keys": ["product_category_name"],
            "type": "full_refresh"
        }
    ]

    # ── Write Silver tables ───────────────────
    for config in silver_tables:
        name   = config["name"]
        df     = config["df"]
        keys   = config["keys"]
        rtype  = config["type"]
        target = SILVER_DELTA_PATH + name

        print(f"\nProcessing: {name} ({rtype})")

        try:
            if rtype == "full_refresh":
                df.write.format("delta") \
                  .mode("overwrite").save(target)
                print(f"✅ Full refresh done")

            elif rtype == "merge_composite":
                merge_composite(df, target, keys)

            else:
                merge_to_silver(
                    df, target, keys[0]
                )

            spark.sql(
                f"OPTIMIZE delta.`{target}`"
            )
            count = df.count()
            print(f"✅ {name} complete")

            results.append({
                "table": name,
                "status": "SUCCESS",
                "records": count,
                "type": rtype
            })

        except Exception as e:
            print(
                f"❌ FAILED {name}: "
                f"{str(e)[:150]}"
            )
            results.append({
                "table": name,
                "status": "FAILED",
                "error": str(e)[:150]
            })

    # ── Summary ───────────────────────────────
    end_time = datetime.now()
    duration = (end_time - start_time).seconds

    print(f"\n{'='*60}")
    print(f"SILVER PIPELINE SUMMARY")
    print(f"Duration: {duration} seconds")
    print(f"{'='*60}")

    total = 0
    for r in results:
        if r["status"] == "SUCCESS":
            total += r["records"]
            print(
                f"✅ {r['table']:<40}"
                f" {r['records']:>10,}"
                f" ({r['type']})"
            )
        else:
            print(
                f"❌ {r['table']:<40} FAILED"
            )

    print(f"\nTotal: {total:,} records")
    print(f"{'='*60}\n")

    return results


# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SilverIngestion") \
        .getOrCreate()

    configure_auth(spark)
    run_silver_pipeline(spark)