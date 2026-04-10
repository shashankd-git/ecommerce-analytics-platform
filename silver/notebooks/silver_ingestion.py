# ============================================
# Silver Layer Ingestion Script
# Project: E-Commerce Analytics Platform
# Description: Reads Bronze Delta tables,
#              cleans and standardizes data,
#              writes Silver Delta tables
#              using MERGE (upsert pattern)
#              History tables for orders
#              and customers track all
#              previous versions of records
# ============================================

# ============================================
# CONFIGURATION
# ============================================

STORAGE_ACCOUNT   = "ecommerceadls2026"
BRONZE_CONTAINER  = "bronze"
SILVER_CONTAINER  = "silver"

# Credentials from Databricks Secret Scopes
# Never stored in code or GitHub
CLIENT_ID     = dbutils.secrets.get(
    scope="ecommerce-scope",
    key="azure-client-id"
)
CLIENT_SECRET = dbutils.secrets.get(
    scope="ecommerce-scope",
    key="azure-client-secret"
)
TENANT_ID     = dbutils.secrets.get(
    scope="ecommerce-scope",
    key="azure-tenant-id"
)

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
    """
    Adds 2 audit columns to every Silver row:
    - silver_updated_at: when Silver processed it
    - silver_pipeline_run_id: which Silver run
    """
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
    MERGE using composite key.
    Used when single column is NOT unique.
    order_items: order_id + order_item_id
    payments:    order_id + payment_sequential
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


def write_history_table(
    spark,
    incoming_df,
    current_path,
    history_path,
    merge_key
):
    """
    Captures changed records BEFORE MERGE
    and writes them to history table.

    Logic:
    1. Check if Silver table exists
       First run = nothing to compare → skip
    2. Read current Silver table
    3. Join current with incoming on primary key
    4. Take OLD version from current table
    5. Stamp valid_from, valid_to, change_type
    6. Append old version to history table

    History table captures:
    - All previous versions of each record
    - When each version was valid
    - What type of change happened
    """

    # First run — no existing Silver table
    if not DeltaTable.isDeltaTable(
        spark, current_path
    ):
        print(
            f"ℹ️  No existing table — "
            f"skipping history"
        )
        return

    # Read current Silver table
    current_df = spark.read \
        .format("delta") \
        .load(current_path)

    # Find records that exist in both
    # current and incoming = potential updates
    changed_count = current_df.alias("current") \
        .join(
            incoming_df.select(merge_key)
            .alias("incoming"),
            on=merge_key,
            how="inner"
        ).count()

    if changed_count == 0:
        print(
            f"ℹ️  No changed records — "
            f"skipping history"
        )
        return

    # Take OLD version from current table
    # These are about to be overwritten by MERGE
    history_df = current_df.alias("current") \
        .join(
            incoming_df.select(merge_key)
            .alias("incoming"),
            on=merge_key,
            how="inner"
        ) \
        .select("current.*") \
        .withColumn(
            "valid_from",
            col("silver_updated_at")
        ) \
        .withColumn(
            "valid_to",
            current_timestamp()
        ) \
        .withColumn(
            "change_type",
            lit("update")
        ) \
        .withColumn(
            "history_created_at",
            current_timestamp()
        )

    # Append old version to history table
    history_df.write \
        .format("delta") \
        .mode("append") \
        .save(history_path)

    print(
        f"✅ {history_df.count():,} records"
        f" written to history"
    )


# ============================================
# TABLE TRANSFORMATIONS
# ============================================

def transform_orders(df):
    """
    - Parse 5 timestamp columns String→Timestamp
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
            to_timestamp(
                "order_delivered_carrier_date",
                "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_delivered_customer_date",
            to_timestamp(
                "order_delivered_customer_date",
                "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_estimated_delivery_date",
            to_timestamp(
                "order_estimated_delivery_date",
                "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_status",
            trim(lower(col("order_status")))) \
        .withColumn("approval_time_minutes",
            when(
                col("order_approved_at").isNotNull(),
                round(
                    (unix_timestamp("order_approved_at") -
                     unix_timestamp(
                         "order_purchase_timestamp"))
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
            "order_id", "customer_id",
            "order_status",
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
    - Validate price > 0
    - Validate freight_value >= 0
    - Calculate total_item_value
    - Parse shipping_limit_date
    NOTE: Composite key order_id + order_item_id
    """
    return df \
        .filter(col("price") > 0) \
        .filter(col("freight_value") >= 0) \
        .withColumn("total_item_value",
            round(
                col("price") +
                col("freight_value"), 2
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
    - Join English category names
    - Fill null categories with 'unknown'
    - Fix typos: lenght → length
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
    SAME PATTERN AS transform_customers
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
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city", "seller_state"
        )


def transform_payments(df):
    """
    - Remove zero/negative payments
    - Standardize payment_type to lowercase
    - Round payment_value to 2 decimal places
    NOTE: Composite key order_id + payment_sequential
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
    - Deduplicate by review_id
    - Validate review_score between 1-5
    - Parse timestamps safely using
      .startswith("20") guard
    - Add has_comment flag
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
            col("review_comment_message").isNotNull()
        ) \
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
    - Filter invalid Brazilian coordinates
    - Standardize city/state
    - Deduplicate by zip code
    NOTE: Full refresh pattern
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
    - Remove nulls
    - Standardize to lowercase
    - Deduplicate
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
    3. Write history BEFORE MERGE
       for orders and customers only
    4. MERGE into Silver tables
    5. Optimize all Delta tables
    6. Print summary
    """

    pipeline_run_id = str(uuid.uuid4())
    start_time      = datetime.now()
    results         = []

    print(f"\n{'='*60}")
    print(f"SILVER PIPELINE STARTED")
    print(f"Run ID:  {pipeline_run_id}")
    print(f"Started: {start_time}")
    print(f"{'='*60}\n")

    # Read Bronze tables
    print("Reading Bronze Delta tables...")

    bronze_orders = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_orders")
    bronze_items = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH +
              "bronze_order_items")
    bronze_customers = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_customers")
    bronze_products = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_products")
    bronze_sellers = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH + "bronze_sellers")
    bronze_payments = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH +
              "bronze_order_payments")
    bronze_reviews = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH +
              "bronze_order_reviews")
    bronze_geolocation = spark.read \
        .format("delta") \
        .load(BRONZE_DELTA_PATH +
              "bronze_geolocation")
    bronze_categories = spark.read.format("delta") \
        .load(BRONZE_DELTA_PATH +
              "bronze_category_translation")

    print(f"✅ All Bronze tables loaded\n")

    # ── Clean up tables with known ────────────
    # duplicate key issues before MERGE
    # Safe to delete — Bronze has raw data
    # Pipeline will recreate fresh
    tables_to_clean = [
            "silver_order_items",
            "silver_payments"
    ]
    for table in tables_to_clean:
        path = SILVER_DELTA_PATH + table
        try:
            if DeltaTable.isDeltaTable(
                spark, path
            ):
                dbutils.fs.rm(path, recurse = True)
                print(f"✅ Cleaned: {table}")
        except Exception as e:
            print(
                f"ℹ️  Could not clean {table}:"
                f" {str(e)[:50]}"
            )        
    print("Applying transformations...")

    df_orders = add_silver_metadata(
        transform_orders(bronze_orders))
    df_items = add_silver_metadata(
        transform_order_items(bronze_items))
    df_customers = add_silver_metadata(
        transform_customers(bronze_customers))
    df_products = add_silver_metadata(
        transform_products(
            bronze_products, bronze_categories))
    df_sellers = add_silver_metadata(
        transform_sellers(bronze_sellers))
    df_payments = add_silver_metadata(
        transform_payments(bronze_payments))
    df_reviews = add_silver_metadata(
        transform_reviews(bronze_reviews))
    df_geolocation = add_silver_metadata(
        transform_geolocation(bronze_geolocation))
    df_categories = add_silver_metadata(
        transform_category_translation(
            bronze_categories))

    print(f"✅ All transformations applied\n")

    # Write history BEFORE MERGE
    # Orders and customers only
    print("Writing history tables...")

    write_history_table(
        spark,
        incoming_df  = df_orders,
        current_path = SILVER_DELTA_PATH +
                       "silver_orders",
        history_path = SILVER_DELTA_PATH +
                       "silver_orders_history",
        merge_key    = "order_id"
    )

    write_history_table(
        spark,
        incoming_df  = df_customers,
        current_path = SILVER_DELTA_PATH +
                       "silver_customers",
        history_path = SILVER_DELTA_PATH +
                       "silver_customers_history",
        merge_key    = "customer_id"
    )

    print(f"✅ History tables written\n")

    # Silver table write config
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
            "keys": [
                "order_id",
                "payment_sequential"
            ],
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
            "keys": [
                "geolocation_zip_code_prefix"
            ],
            "type": "full_refresh"
        },
        {
            "name": "silver_categories",
            "df": df_categories,
            "keys": ["product_category_name"],
            "type": "full_refresh"
        }
    ]

    # Write all Silver tables
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
                f"❌ FAILED {name}:"
                f" {str(e)[:150]}"
            )
            results.append({
                "table": name,
                "status": "FAILED",
                "error": str(e)[:150]
            })

    # Summary
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