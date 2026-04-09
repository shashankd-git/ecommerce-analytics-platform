
STORAGE_ACCOUNT = "ecommerceadls2026"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
CLIENT_ID = ""
CLIENT_SECRET = ""
TENANT_ID = ""

BRONZE_DELTA_PATH = (
    f"abfss://{BRONZE_CONTAINER}"
    f"@{STORAGE_ACCOUNT}"
    f".dfs.core.windows.net/"
    f"_delta_tables/"
)

SILVER_DELTA_PATH = (
    f"abfss://{SILVER_CONTAINER}"
    f"@{STORAGE_ACCOUNT}"
    f".dfs.core.windows.net/"
    f"_delta_tables/"
)


# ============================================
# IMPORTS
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower,
    when, lit, coalesce,
    to_timestamp, current_timestamp,
    current_date, year, month,
    round, abs, regexp_replace,
    concat_ws, sha2, datediff,
    unix_timestamp, from_unixtime
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

def add_silver_metadata(df, pipeline_run_id):
    """Add Silver layer audit columns"""
    return df \
        .withColumn(
            "silver_updated_at",
            current_timestamp()
        ) \
        .withColumn(
            "silver_pipeline_run_id",
            lit(pipeline_run_id)
        )

def merge_to_silver(
    spark, df, target_path,
    merge_key, partition_cols=None
):
    """
    MERGE new data into Silver Delta table.
    INSERT new records.
    UPDATE changed records.
    Never deletes — preserves history.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(
            spark, target_path
        )
        delta_table.alias("target").merge(
            df.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        print(f"✅ MERGE complete: {target_path}")
    else:
        write_kwargs = dict(
            format="delta",
            mode="overwrite"
        )
        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)
        print(f"✅ Initial write: {target_path}")


# ============================================
# TABLE TRANSFORMATIONS
# ============================================

def transform_orders(df):
    """
    Clean olist_orders table:
    - Parse timestamps String → Timestamp
    - Standardize order_status
    - Calculate delivery metrics
    """
    return df \
        .dropDuplicates(["order_id"]) \
        .withColumn(
            "order_purchase_timestamp",
            to_timestamp(
                col("order_purchase_timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .withColumn(
            "order_approved_at",
            to_timestamp(
                col("order_approved_at"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .withColumn(
            "order_delivered_carrier_date",
            to_timestamp(
                col("order_delivered_carrier_date"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .withColumn(
            "order_delivered_customer_date",
            to_timestamp(
                col("order_delivered_customer_date"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .withColumn(
            "order_estimated_delivery_date",
            to_timestamp(
                col("order_estimated_delivery_date"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .withColumn(
            "order_status",
            trim(lower(col("order_status")))
        ) \
        .withColumn(
            "approval_time_minutes",
            when(
                col("order_approved_at").isNotNull(),
                round(
                    (unix_timestamp("order_approved_at") -
                     unix_timestamp("order_purchase_timestamp"))
                    / 60, 2
                )
            ).otherwise(None)
        ) \
        .withColumn(
            "delivery_days",
            when(
                col("order_delivered_customer_date").isNotNull(),
                datediff(
                    col("order_delivered_customer_date"),
                    col("order_purchase_timestamp")
                )
            ).otherwise(None)
        ) \
        .withColumn(
            "is_late_delivery",
            when(
                col("order_delivered_customer_date").isNotNull() &
                col("order_estimated_delivery_date").isNotNull(),
                col("order_delivered_customer_date") >
                col("order_estimated_delivery_date")
            ).otherwise(None)
        ) \
        .select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "approval_time_minutes",
            "delivery_days",
            "is_late_delivery"
        )


def transform_order_items(df):
    """
    Clean olist_order_items table:
    - Validate price > 0
    - Validate freight_value >= 0
    - Calculate total_item_value
    """
    return df \
        .filter(col("price") > 0) \
        .filter(col("freight_value") >= 0) \
        .withColumn(
            "total_item_value",
            round(
                col("price") + col("freight_value"), 2
            )
        ) \
        .withColumn(
            "shipping_limit_date",
            to_timestamp(
                col("shipping_limit_date"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .select(
            "order_id", "order_item_id",
            "product_id", "seller_id",
            "shipping_limit_date",
            "price", "freight_value",
            "total_item_value"
        )


def transform_customers(df):
    """
    Clean olist_customers table:
    - Standardize city to title case
    - Standardize state to uppercase
    - Remove leading/trailing spaces
    """
    return df \
        .dropDuplicates(["customer_id"]) \
        .withColumn(
            "customer_city",
            trim(lower(col("customer_city")))
        ) \
        .withColumn(
            "customer_state",
            trim(upper(col("customer_state")))
        ) \
        .withColumn(
            "customer_zip_code_prefix",
            trim(col("customer_zip_code_prefix"))
        ) \
        .filter(col("customer_id").isNotNull()) \
        .select(
            "customer_id", "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city", "customer_state"
        )


def transform_products(df, category_translation_df):
    """
    Clean olist_products table:
    - Fix column name typos
    - Fill null categories
    - Join English category names
    - Validate dimensions
    """
    # Join English category names
    df_with_english = df.join(
        category_translation_df.select(
            "product_category_name",
            "product_category_name_english"
        ),
        on="product_category_name",
        how="left"
    )

    return df_with_english \
        .dropDuplicates(["product_id"]) \
        .withColumn(
            "product_category_name",
            coalesce(
                col("product_category_name"),
                lit("unknown")
            )
        ) \
        .withColumn(
            "product_category_name_english",
            coalesce(
                col("product_category_name_english"),
                lit("unknown")
            )
        ) \
        .withColumnRenamed(
            "product_name_lenght",
            "product_name_length"
        ) \
        .withColumnRenamed(
            "product_description_lenght",
            "product_description_length"
        ) \
        .filter(
            col("product_weight_g").isNull() |
            (col("product_weight_g") > 0)
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
    Clean olist_sellers table:
    - Standardize city/state
    """
    return df \
        .dropDuplicates(["seller_id"]) \
        .withColumn(
            "seller_city",
            trim(lower(col("seller_city")))
        ) \
        .withColumn(
            "seller_state",
            trim(upper(col("seller_state")))
        ) \
        .filter(col("seller_id").isNotNull()) \
        .select(
            "seller_id", "seller_zip_code_prefix",
            "seller_city", "seller_state"
        )


def transform_payments(df):
    """
    Clean olist_order_payments table:
    - Validate payment values
    - Standardize payment types
    """
    return df \
        .filter(col("payment_value") > 0) \
        .withColumn(
            "payment_type",
            trim(lower(col("payment_type")))
        ) \
        .withColumn(
            "payment_value",
            round(col("payment_value"), 2)
        ) \
        .select(
            "order_id", "payment_sequential",
            "payment_type", "payment_installments",
            "payment_value"
        )


def transform_reviews(df):
    """
    Clean olist_order_reviews table:
    - Deduplicate by review_id
    - Validate review_score 1-5
    - Parse timestamps
    - Keep review text as-is
      (sentiment analysis in Gold layer)
    """
    return df \
        .dropDuplicates(["review_id"]) \
        .filter(
            col("review_score").between(1, 5)
        ) \
        .withColumn(
            "review_creation_date",
            to_timestamp(
                col("review_creation_date"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .withColumn(
            "review_answer_timestamp",
            to_timestamp(
                col("review_answer_timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            )
        ) \
        .withColumn(
            "has_comment",
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
    Clean olist_geolocation table:
    - Deduplicate by zip code
      (keep one coordinate per zip)
    - Standardize city/state
    - Filter invalid coordinates
    """
    return df \
        .filter(
            col("geolocation_lat").between(-34, 6)
        ) \
        .filter(
            col("geolocation_lng").between(-74, -32)
        ) \
        .withColumn(
            "geolocation_city",
            trim(lower(col("geolocation_city")))
        ) \
        .withColumn(
            "geolocation_state",
            trim(upper(col("geolocation_state")))
        ) \
        .dropDuplicates(["geolocation_zip_code_prefix"]) \
        .select(
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        )


def transform_category_translation(df):
    """
    Clean category_translation table:
    - Remove nulls
    - Standardize to lowercase
    """
    return df \
        .dropDuplicates(["product_category_name"]) \
        .filter(
            col("product_category_name").isNotNull()
        ) \
        .withColumn(
            "product_category_name",
            trim(lower(col("product_category_name")))
        ) \
        .withColumn(
            "product_category_name_english",
            trim(lower(
                col("product_category_name_english")
            ))
        ) \
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
    1. Read all Bronze Delta tables
    2. Apply transformations
    3. MERGE into Silver Delta tables
    4. Print summary
    """

    pipeline_run_id = str(uuid.uuid4())
    start_time      = datetime.now()
    results         = []

    print(f"\n{'='*60}")
    print(f"SILVER PIPELINE STARTED")
    print(f"Run ID:  {pipeline_run_id}")
    print(f"Started: {start_time}")
    print(f"{'='*60}\n")

    # ── Step 1: Read all Bronze tables ────────
    print("Reading Bronze Delta tables...")

    bronze = {
        "orders": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_orders"),
        "order_items": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_order_items"),
        "customers": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_customers"),
        "products": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_products"),
        "sellers": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_sellers"),
        "payments": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_order_payments"),
        "reviews": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_order_reviews"),
        "geolocation": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_geolocation"),
        "category_translation": spark.read.format("delta")
            .load(BRONZE_DELTA_PATH + "bronze_category_translation")
    }

    print(f"✅ Read {len(bronze)} Bronze tables\n")

    # ── Step 2: Define Silver table configs ───
    # Each entry:
    # transform_fn = function to clean data
    # merge_key    = primary key for MERGE
    # target       = Silver Delta table path
    # refresh_type = merge or full_refresh

    silver_config = [
        {
            "name": "silver_orders",
            "df": transform_orders(bronze["orders"]),
            "merge_key": "order_id",
            "target": SILVER_DELTA_PATH + "silver_orders",
            "refresh_type": "merge"
        },
        {
            "name": "silver_order_items",
            "df": transform_order_items(bronze["order_items"]),
            "merge_key": "order_id",
            "target": SILVER_DELTA_PATH + "silver_order_items",
            "refresh_type": "merge"
        },
        {
            "name": "silver_customers",
            "df": transform_customers(bronze["customers"]),
            "merge_key": "customer_id",
            "target": SILVER_DELTA_PATH + "silver_customers",
            "refresh_type": "merge"
        },
        {
            "name": "silver_products",
            "df": transform_products(
                bronze["products"],
                bronze["category_translation"]
            ),
            "merge_key": "product_id",
            "target": SILVER_DELTA_PATH + "silver_products",
            "refresh_type": "merge"
        },
        {
            "name": "silver_sellers",
            "df": transform_sellers(bronze["sellers"]),
            "merge_key": "seller_id",
            "target": SILVER_DELTA_PATH + "silver_sellers",
            "refresh_type": "merge"
        },
        {
            "name": "silver_payments",
            "df": transform_payments(bronze["payments"]),
            "merge_key": "order_id",
            "target": SILVER_DELTA_PATH + "silver_payments",
            "refresh_type": "merge"
        },
        {
            "name": "silver_reviews",
            "df": transform_reviews(bronze["reviews"]),
            "merge_key": "review_id",
            "target": SILVER_DELTA_PATH + "silver_reviews",
            "refresh_type": "merge"
        },
        {
            "name": "silver_geolocation",
            "df": transform_geolocation(
                bronze["geolocation"]
            ),
            "merge_key": "geolocation_zip_code_prefix",
            "target": SILVER_DELTA_PATH + "silver_geolocation",
            "refresh_type": "full_refresh"
        },
        {
            "name": "silver_category_translation",
            "df": transform_category_translation(
                bronze["category_translation"]
            ),
            "merge_key": "product_category_name",
            "target": SILVER_DELTA_PATH + "silver_category_translation",
            "refresh_type": "full_refresh"
        }
    ]

    # ── Step 3: Process each Silver table ─────
    for config in silver_config:
        print(f"\nProcessing: {config['name']}")
        print(f"Refresh type: {config['refresh_type']}")

        try:
            # Add Silver metadata columns
            df_silver = add_silver_metadata(
                config["df"], pipeline_run_id
            )

            record_count = df_silver.count()
            print(f"Records to process: {record_count:,}")

            if config["refresh_type"] == "full_refresh":
                # Full refresh — overwrite entire table
                df_silver.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(config["target"])
                print(f"✅ Full refresh complete")

            else:
                # MERGE — insert new, update changed
                merge_to_silver(
                    spark,
                    df_silver,
                    config["target"],
                    config["merge_key"]
                )

            # Optimize after write
            spark.sql(
                f"OPTIMIZE delta.`{config['target']}`"
            )
            print(f"✅ {config['name']} complete")

            results.append({
                "table": config["name"],
                "status": "SUCCESS",
                "records": record_count,
                "refresh_type": config["refresh_type"]
            })

        except Exception as e:
            print(f"❌ FAILED {config['name']}: {str(e)}")
            results.append({
                "table": config["name"],
                "status": "FAILED",
                "error": str(e)
            })

    # ── Step 4: Print Summary ─────────────────
    end_time = datetime.now()
    duration = (end_time - start_time).seconds

    print(f"\n{'='*60}")
    print(f"SILVER PIPELINE SUMMARY")
    print(f"Duration: {duration} seconds")
    print(f"{'='*60}")

    total_records = 0
    for r in results:
        if r["status"] == "SUCCESS":
            total_records += r["records"]
            print(
                f"✅ {r['table']:<40}"
                f" {r['records']:>10,} records"
                f" ({r['refresh_type']})"
            )
        else:
            print(
                f"❌ {r['table']:<40}"
                f" FAILED: {r.get('error','')[:50]}"
            )

    print(f"\nTotal records: {total_records:,}")
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