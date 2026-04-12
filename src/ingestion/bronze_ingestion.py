import os
import sys
sys.path.append(r"D:\pharma_pipeline")

from config.spark_session import get_spark_session
from config.settings import RAW_DIR, BRONZE_DIR

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType, DateType
)

# Define explicit schema — never infer schema in production pipelines
RAW_SCHEMA = StructType([
    StructField("record_id",    StringType(),  nullable=False),
    StructField("sale_date",    StringType(),  nullable=True),
    StructField("product",      StringType(),  nullable=True),
    StructField("category",     StringType(),  nullable=True),
    StructField("subject_area", StringType(),  nullable=True),
    StructField("sales_team",   StringType(),  nullable=True),
    StructField("zip_code",     StringType(),  nullable=True),
    StructField("account_id",   StringType(),  nullable=True),
    StructField("units_sold",   IntegerType(), nullable=True),
    StructField("sales_amount", DoubleType(),  nullable=True),
    StructField("revenue",      DoubleType(),  nullable=True),
    StructField("region",       StringType(),  nullable=True),
    StructField("rep_id",       StringType(),  nullable=True),
])


def ingest_to_bronze(spark: SparkSession) -> None:
    raw_path = os.path.join(RAW_DIR, "pharma_sales_raw.csv")
    bronze_path = os.path.join(BRONZE_DIR, "pharma_sales")

    print(f"Reading raw data from: {raw_path}")

    df = spark.read \
        .option("header", "true") \
        .schema(RAW_SCHEMA) \
        .csv(raw_path)

    # Add ingestion metadata columns
    df = df \
        .withColumn("ingested_at", F.current_timestamp()) \
        .withColumn("source_file", F.lit("pharma_sales_raw.csv")) \
        .withColumn("pipeline_version", F.lit("1.0.0"))

    # Partition by category so reads are faster downstream
    df.write \
        .mode("overwrite") \
        .partitionBy("category") \
        .parquet(bronze_path)

    print(f"Bronze layer written to: {bronze_path}")
    print(f"Total records: {df.count()}")
    print(f"Partitions: {df.rdd.getNumPartitions()}")
    print("\nSchema:")
    df.printSchema()
    print("\nSample:")
    df.show(3, truncate=False)


if __name__ == "__main__":
    spark = get_spark_session("Bronze_Ingestion")
    ingest_to_bronze(spark)
    spark.stop()