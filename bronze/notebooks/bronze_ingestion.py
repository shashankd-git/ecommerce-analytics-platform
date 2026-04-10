# ============================================
# Bronze Layer Ingestion Script
# Project: E-Commerce Analytics Platform
# Description: Reads raw Olist CSV files
#              from ADLS Gen2 Bronze container
#              Adds metadata, quality checks,
#              writes as Delta tables
# ============================================

# ============================================
# CONFIGURATION
# ============================================

STORAGE_ACCOUNT  = "ecommerceadls2026"
BRONZE_CONTAINER = "bronze"

# Credentials from Databricks Secret Scope
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

BRONZE_PATH = (
    f"abfss://{BRONZE_CONTAINER}"
    f"@{STORAGE_ACCOUNT}"
    f".dfs.core.windows.net/"
)
BRONZE_DELTA_PATH = (
    f"abfss://{BRONZE_CONTAINER}"
    f"@{STORAGE_ACCOUNT}"
    f".dfs.core.windows.net/"
    f"_delta_tables/"
)
DEAD_LETTER_PATH = (
    f"abfss://{BRONZE_CONTAINER}"
    f"@{STORAGE_ACCOUNT}"
    f".dfs.core.windows.net/"
    f"_dead_letter/"
)

# ============================================
# IMPORTS
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, current_date,
    year, month, lit, sha2, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from datetime import datetime
import uuid
import sys

# ============================================
# SCHEMA DEFINITIONS
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

category_translation_schema = StructType([
    StructField("product_category_name",
        StringType(), False),
    StructField("product_category_name_english",
        StringType(), True)
])

# ============================================
# MASTER CONFIG
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

# ============================================
# HELPER FUNCTIONS
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


def add_bronze_metadata(df, source_file,
                        pipeline_run_id):
    """
    Adds 8 audit columns to every Bronze row:
    - ingestion_timestamp: exact load time
    - ingestion_date: date of load
    - ingestion_year: partition column
    - ingestion_month: partition column
    - source_file_name: which CSV file
    - source_system: which source system
    - pipeline_run_id: which pipeline run
    - record_hash: SHA256 fingerprint of row
    """
    original_cols = [
        c for c in df.columns
        if not c.startswith("_")
    ]
    return df \
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        ) \
        .withColumn(
            "ingestion_date",
            current_date()
        ) \
        .withColumn(
            "ingestion_year",
            year(current_date())
        ) \
        .withColumn(
            "ingestion_month",
            month(current_date())
        ) \
        .withColumn(
            "source_file_name",
            lit(source_file)
        ) \
        .withColumn(
            "source_system",
            lit("olist_kaggle")
        ) \
        .withColumn(
            "pipeline_run_id",
            lit(pipeline_run_id)
        ) \
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "|",
                    *[col(c) for c in original_cols]
                ),
                256
            )
        )


def split_good_bad_records(df, primary_key):
    """
    Splits DataFrame into two:
    good_records: valid primary key + no corruption
    bad_records:  null/empty key OR corrupted row
    """
    if "_corrupt_record" in df.columns:
        good_records = df.filter(
            col("_corrupt_record").isNull() &
            col(primary_key).isNotNull() &
            (col(primary_key) != "")
        ).drop("_corrupt_record")

        bad_records = df.filter(
            col("_corrupt_record").isNotNull() |
            col(primary_key).isNull() |
            (col(primary_key) == "")
        ).withColumn(
            "error_timestamp",
            current_timestamp()
        )
    else:
        good_records = df.filter(
            col(primary_key).isNotNull() &
            (col(primary_key) != "")
        )
        bad_records = df.filter(
            col(primary_key).isNull() |
            (col(primary_key) == "")
        ).withColumn(
            "error_timestamp",
            current_timestamp()
        )

    return good_records, bad_records


def run_quality_checks(df, table_name,
                       primary_key):
    """
    Counts quality issues — does NOT fix them.
    Fixing is Silver layer responsibility.
    """
    total = df.count()
    null_keys = df.filter(
        col(primary_key).isNull()
    ).count()
    duplicates = total - df.dropDuplicates(
        [primary_key]
    ).count()

    print(f"\n{'='*50}")
    print(f"Quality Report: {table_name}")
    print(f"{'='*50}")
    print(f"Total records:     {total:>10,}")
    print(f"Null primary keys: {null_keys:>10,}")
    print(f"Duplicate keys:    {duplicates:>10,}")

    for column in df.columns:
        null_count = df.filter(
            col(column).isNull()
        ).count()
        if null_count > 0:
            pct = (null_count / total * 100)
            print(
                f"  Nulls in {column:<40}"
                f" {null_count:>8,} ({pct:.1f}%)"
            )

    return {
        "table_name": table_name,
        "total_records": total,
        "null_primary_keys": null_keys,
        "duplicate_keys": duplicates
    }


def write_bronze_delta(df, target_path):
    """
    Writes DataFrame as partitioned Delta table.
    mode=append: never overwrites existing data
    partitionBy: organizes files by year/month
    """
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy(
            "ingestion_year",
            "ingestion_month"
        ) \
        .save(target_path)
    print(f"✅ Written to: {target_path}")


def write_dead_letter(df, dead_letter_path,
                      table_name):
    """
    Writes rejected records to dead letter queue.
    Pipeline continues even if bad records exist.
    """
    count = df.count()
    if count > 0:
        df.withColumn(
            "source_table", lit(table_name)
        ).write \
            .format("delta") \
            .mode("append") \
            .save(dead_letter_path)
        print(
            f"⚠️  {count} bad records"
            f" → dead letter queue"
        )
    else:
        print(f"✅ No bad records")


# ============================================
# MAIN PIPELINE
# ============================================

def run_bronze_pipeline(spark):
    """
    Main Bronze pipeline:
    1. Loop through all 9 Olist tables
    2. Read CSV with schema enforcement
    3. Split good vs bad records
    4. Add metadata to good records
    5. Write good records as Delta table
    6. Write bad records to dead letter
    7. Run quality checks
    8. Optimize Delta table
    9. Print summary
    """

    pipeline_run_id = str(uuid.uuid4())
    start_time      = datetime.now()
    quality_results = []

    print(f"\n{'='*60}")
    print(f"BRONZE INGESTION PIPELINE STARTED")
    print(f"Run ID:  {pipeline_run_id}")
    print(f"Started: {start_time}")
    print(f"{'='*60}\n")

    for table_name, config in \
            OLIST_TABLE_CONFIG.items():

        print(f"\nProcessing: {table_name}")
        source_path = (
            BRONZE_PATH + config["source_file"]
        )
        target_path = (
            BRONZE_DELTA_PATH +
            config["target_table"]
        )
        primary_key = config["primary_key"]

        try:
            # Add _corrupt_record to schema
            base_schema     = config["schema"]
            extended_fields = (
                list(base_schema.fields) +
                [StructField(
                    "_corrupt_record",
                    StringType(), True
                )]
            )
            extended_schema = StructType(
                extended_fields
            )

            # Read CSV with schema enforcement
            df_raw = spark.read \
                .format("csv") \
                .option("header", "true") \
                .option("mode", "PERMISSIVE") \
                .option(
                    "columnNameOfCorruptRecord",
                    "_corrupt_record"
                ) \
                .schema(extended_schema) \
                .load(source_path)

            # Split good and bad records
            good_df, bad_df = \
                split_good_bad_records(
                    df_raw, primary_key
                )

            good_count = good_df.count()
            bad_count  = bad_df.count()
            print(
                f"Good: {good_count:,}"
                f" | Bad: {bad_count:,}"
            )

            # Add metadata to good records
            good_df = add_bronze_metadata(
                good_df,
                config["source_file"],
                pipeline_run_id
            )

            # Write good records as Delta table
            write_bronze_delta(
                good_df, target_path
            )

            # Write bad records to dead letter
            write_dead_letter(
                bad_df,
                DEAD_LETTER_PATH,
                table_name
            )

            # Run quality checks
            quality = run_quality_checks(
                good_df,
                table_name,
                primary_key
            )
            quality["bad_records"] = bad_count
            quality_results.append(quality)

            # Optimize Delta table
            spark.sql(
                f"OPTIMIZE delta.`{target_path}`"
            )
            print(f"✅ {table_name} complete")

        except Exception as e:
            print(
                f"❌ FAILED {table_name}:"
                f" {str(e)[:150]}"
            )
            quality_results.append({
                "table_name": table_name,
                "status": "FAILED",
                "error": str(e)[:150]
            })

    # Summary
    end_time      = datetime.now()
    duration      = (end_time - start_time).seconds
    total_records = 0
    total_bad     = 0

    print(f"\n{'='*60}")
    print(f"BRONZE PIPELINE SUMMARY")
    print(f"Duration: {duration} seconds")
    print(f"{'='*60}")

    for result in quality_results:
        if result.get("status") == "FAILED":
            print(
                f"❌ {result['table_name']:<40}"
                f" FAILED"
            )
        else:
            records = result.get(
                "total_records", 0
            )
            bad = result.get("bad_records", 0)
            total_records += records
            total_bad     += bad
            print(
                f"✅ {result['table_name']:<40}"
                f" {records:>10,} records"
                f" ({bad} bad)"
            )

    print(f"\nTotal records: {total_records:,}")
    print(f"Total bad:     {total_bad:,}")
    print(f"{'='*60}\n")


# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .getOrCreate()

    configure_auth(spark)
    run_bronze_pipeline(spark)