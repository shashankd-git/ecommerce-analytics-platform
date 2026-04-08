# ============================================
# Bronze Layer Ingestion Script
# Project: E-Commerce Analytics Platform
# Author: Shashank
# Description: Reads raw Olist CSV files
#              from ADLS Gen2 Bronze container
#              Adds metadata, quality checks,
#              writes as Delta tables
# ============================================

import uuid
import sys
from datetime import datetime
from pyspark.sql.functions import col

# ============================================
# CONFIGURATION
# Update these values for your environment
# ============================================

# Storage config
STORAGE_ACCOUNT = "ecommerceadls2026"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

# Service Principal config
# In production use Key Vault
# For now using variables
CLIENT_ID     = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"
TENANT_ID     = "YOUR_TENANT_ID"

# Path config
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

# Pipeline run ID — unique per execution
PIPELINE_RUN_ID = str(uuid.uuid4())

# ============================================
# STEP 1: Configure Spark Authentication
# ============================================

def configure_spark_auth(spark):
    """
    Configure Service Principal OAuth
    authentication for ADLS Gen2 access
    """
    print("Configuring Spark authentication...")

    base_config = (
        f"fs.azure.account"
        f".{{}}.{STORAGE_ACCOUNT}"
        f".dfs.core.windows.net"
    )

    spark.conf.set(
        f"fs.azure.account.auth.type"
        f".{STORAGE_ACCOUNT}"
        f".dfs.core.windows.net",
        "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type"
        f".{STORAGE_ACCOUNT}"
        f".dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs"
        ".oauth2.ClientCredsTokenProvider"
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id"
        f".{STORAGE_ACCOUNT}"
        f".dfs.core.windows.net",
        CLIENT_ID
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret"
        f".{STORAGE_ACCOUNT}"
        f".dfs.core.windows.net",
        CLIENT_SECRET
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint"
        f".{STORAGE_ACCOUNT}"
        f".dfs.core.windows.net",
        f"https://login.microsoftonline.com"
        f"/{TENANT_ID}/oauth2/token"
    )

    print("✅ Spark authentication configured")


# ============================================
# STEP 2: Read CSV with Schema Enforcement
# ============================================

def read_csv_with_schema(spark, file_path, schema):
    """
    Reads CSV file with:
    - Explicit schema enforcement
    - Bad record capture in _corrupt_record
    - Header row handling
    """
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option(
            "columnNameOfCorruptRecord",
            "_corrupt_record"
        ) \
        .schema(schema) \
        .load(file_path)


# ============================================
# STEP 3: Process single table
# ============================================

def process_bronze_table(
    spark,
    table_name,
    config,
    quality_results
):
    """
    Full Bronze processing for one table:
    1. Read CSV with schema enforcement
    2. Split good vs bad records
    3. Add metadata to good records
    4. Write good records as Delta table
    5. Write bad records to dead letter
    6. Run quality checks
    7. Optimize Delta table
    """

    print(f"\n{'='*60}")
    print(f"Processing: {table_name}")
    print(f"Source: {config['source_file']}")
    print(f"{'='*60}")

    source_path = BRONZE_PATH + config["source_file"]
    target_path = BRONZE_DELTA_PATH + config["target_table"]
    primary_key = config["primary_key"]

    try:
        # ── Read raw CSV ──────────────────────
        print(f"Reading CSV from: {source_path}")
        df_raw = read_csv_with_schema(
            spark,
            source_path,
            config["schema"]
        )

        # ── Split good/bad records ────────────
        from bronze.utils.bronze_utils import (
            split_good_bad_records,
            add_bronze_metadata,
            write_bronze_delta,
            write_dead_letter,
            run_quality_checks,
            optimize_delta_table
        )

        good_df, bad_df = split_good_bad_records(
            df_raw, primary_key
        )

        good_count = good_df.count()
        bad_count  = bad_df.count()

        print(f"Good records: {good_count:,}")
        print(f"Bad records:  {bad_count:,}")

        # ── Add metadata ──────────────────────
        good_df = add_bronze_metadata(
            good_df,
            config["source_file"],
            PIPELINE_RUN_ID
        )

        # ── Write good records ────────────────
        write_bronze_delta(good_df, target_path)

        # ── Write bad records ─────────────────
        if bad_count > 0:
            write_dead_letter(
                bad_df,
                DEAD_LETTER_PATH,
                table_name
            )

        # ── Quality checks ────────────────────
        quality = run_quality_checks(
            good_df,
            table_name,
            primary_key
        )
        quality["bad_records"] = bad_count
        quality_results.append(quality)

        # ── Optimize Delta table ──────────────
        optimize_delta_table(
            spark,
            target_path,
            zorder_cols=[primary_key]
        )

        print(f"✅ {table_name} complete")
        return True

    except Exception as e:
        print(f"❌ ERROR processing {table_name}: {str(e)}")
        quality_results.append({
            "table_name": table_name,
            "status": "FAILED",
            "error": str(e),
            "check_timestamp": datetime.now().isoformat()
        })
        return False


# ============================================
# STEP 4: Print Summary Report
# ============================================

def print_summary(quality_results, start_time):
    """
    Prints pipeline execution summary
    """
    end_time = datetime.now()
    duration = (end_time - start_time).seconds

    print(f"\n{'='*60}")
    print(f"BRONZE INGESTION SUMMARY")
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Duration: {duration} seconds")
    print(f"{'='*60}")

    total_records = 0
    total_bad     = 0
    failed_tables = []

    for result in quality_results:
        if result.get("status") == "FAILED":
            failed_tables.append(
                result["table_name"]
            )
            print(
                f"❌ {result['table_name']:<40}"
                f" FAILED: {result.get('error','')}"
            )
        else:
            records = result.get(
                "total_records", 0
            )
            bad     = result.get("bad_records", 0)
            total_records += records
            total_bad     += bad
            print(
                f"✅ {result['table_name']:<40}"
                f" {records:>10,} records"
                f" ({bad} bad)"
            )

    print(f"\n{'='*60}")
    print(f"Total records loaded: {total_records:>10,}")
    print(f"Total bad records:    {total_bad:>10,}")
    print(f"Failed tables:        {len(failed_tables)}")

    if failed_tables:
        print(f"Failed: {failed_tables}")

    status = "SUCCESS" if not failed_tables \
        else "PARTIAL FAILURE"
    print(f"Pipeline Status:      {status}")
    print(f"{'='*60}\n")


# ============================================
# MAIN — Entry point
# ============================================

def main():
    """
    Main Bronze ingestion pipeline:
    1. Configure authentication
    2. Process all 9 Olist tables
    3. Print summary report
    """

    print(f"\n{'='*60}")
    print(f"BRONZE INGESTION PIPELINE STARTED")
    print(f"Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    print(f"{'='*60}\n")

    start_time = datetime.now()

    # Import schemas and config
    from bronze.schemas.olist_schemas import (
        OLIST_TABLE_CONFIG
    )

    # Get active Spark session
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .getOrCreate()

    # Configure authentication
    configure_spark_auth(spark)

    # Process all tables
    quality_results = []
    success_count   = 0
    failed_count    = 0

    for table_name, config in \
            OLIST_TABLE_CONFIG.items():
        success = process_bronze_table(
            spark,
            table_name,
            config,
            quality_results
        )
        if success:
            success_count += 1
        else:
            failed_count += 1

    # Print summary
    print_summary(quality_results, start_time)

    # Exit with error code if any failures
    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()