# ============================================
# Bronze Layer Utility Functions
# Project: E-Commerce Analytics Platform
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, current_date,
    year, month, lit, sha2,
    concat_ws, col
)
import uuid
from datetime import datetime


# ============================================
# Add metadata columns to Bronze DataFrame
# ============================================
def add_bronze_metadata(df, source_file, pipeline_run_id):
    """
    Adds standard metadata columns to every
    Bronze table for audit and traceability.

    Columns added:
    - ingestion_timestamp: when record was loaded
    - ingestion_date: date of ingestion
    - ingestion_year: year partition
    - ingestion_month: month partition
    - source_file_name: original CSV filename
    - source_system: where data came from
    - pipeline_run_id: unique ID for this run
    - record_hash: hash of all columns
                   used to detect changes
    """
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
            sha2(concat_ws("|", *[
                col(c) for c in df.columns
            ]), 256)
        )


# ============================================
# Split good and bad records
# ============================================
def split_good_bad_records(df, primary_key):
    """
    Separates records into:
    - good_records: valid records for processing
    - bad_records: corrupt/invalid records
                   for dead letter queue

    A record is bad if:
    - _corrupt_record column is not null
      (schema mismatch)
    - primary key is null or empty
      (cannot be joined or tracked)
    """
    # Bad records — schema corruption
    corrupt_records = df.filter(
        col("_corrupt_record").isNotNull()
    )

    # Bad records — null primary key
    null_key_records = df.filter(
        col("_corrupt_record").isNull() &
        col(primary_key).isNull()
    )

    # Bad records — empty primary key
    empty_key_records = df.filter(
        col("_corrupt_record").isNull() &
        (col(primary_key) == "")
    )

    # Good records — valid primary key
    # and no corruption
    good_records = df.filter(
        col("_corrupt_record").isNull() &
        col(primary_key).isNotNull() &
        (col(primary_key) != "")
    ).drop("_corrupt_record")

    # Combine all bad records
    bad_records = corrupt_records \
        .union(null_key_records) \
        .union(empty_key_records) \
        .withColumn(
            "error_timestamp",
            current_timestamp()
        ) \
        .withColumn(
            "error_type",
            lit("corrupt_or_invalid_key")
        )

    return good_records, bad_records


# ============================================
# Run quality checks on Bronze DataFrame
# ============================================
def run_quality_checks(df, table_name, primary_key):
    """
    Runs basic quality checks on Bronze data.
    Does NOT fix anything — just reports.
    Silver layer does the fixing.

    Checks:
    - Total record count
    - Null primary key count
    - Duplicate primary key count
    - Null value counts per column
    """
    print(f"\n{'='*50}")
    print(f"Quality Report: {table_name}")
    print(f"{'='*50}")

    # Total records
    total = df.count()
    print(f"Total records:     {total:>10,}")

    # Null primary keys
    null_keys = df.filter(
        col(primary_key).isNull()
    ).count()
    print(f"Null primary keys: {null_keys:>10,}")

    # Duplicate primary keys
    duplicates = total - df.dropDuplicates(
        [primary_key]
    ).count()
    print(f"Duplicate keys:    {duplicates:>10,}")

    # Null counts per column
    print(f"\nNull counts per column:")
    for column in df.columns:
        null_count = df.filter(
            col(column).isNull()
        ).count()
        if null_count > 0:
            pct = (null_count / total * 100)
            print(
                f"  {column:<45} "
                f"{null_count:>8,} "
                f"({pct:.1f}%)"
            )

    print(f"{'='*50}\n")

    return {
        "table_name": table_name,
        "total_records": total,
        "null_primary_keys": null_keys,
        "duplicate_keys": duplicates,
        "check_timestamp": datetime.now()
            .isoformat()
    }


# ============================================
# Write DataFrame to Delta table
# ============================================
def write_bronze_delta(
    df,
    target_path,
    partition_cols=["ingestion_year", "ingestion_month"]
):
    """
    Writes Bronze DataFrame as Delta table.
    Partitioned by year and month for
    query performance.

    Uses append mode — Bronze never overwrites
    existing data. New data always appended.
    """
    df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy(*partition_cols) \
        .save(target_path)

    print(f"✅ Written to Delta: {target_path}")


# ============================================
# Write bad records to dead letter queue
# ============================================
def write_dead_letter(df, dead_letter_path, table_name):
    """
    Writes rejected records to dead letter queue.
    Operations team can investigate and reprocess.
    """
    if df.count() > 0:
        df.withColumn(
            "source_table", lit(table_name)
        ).write \
            .format("delta") \
            .mode("append") \
            .save(dead_letter_path)

        print(
            f"⚠️  {df.count()} bad records "
            f"written to dead letter queue"
        )
    else:
        print(f"✅ No bad records for {table_name}")


# ============================================
# Optimize Delta table
# ============================================
def optimize_delta_table(spark, table_path, zorder_cols=None):
    """
    Optimizes Delta table for query performance:
    - OPTIMIZE: compacts small files
    - ZORDER: co-locates related data
    """
    spark.sql(f"OPTIMIZE delta.`{table_path}`")

    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(
            f"OPTIMIZE delta.`{table_path}` "
            f"ZORDER BY ({cols})"
        )

    print(f"✅ Optimized: {table_path}")