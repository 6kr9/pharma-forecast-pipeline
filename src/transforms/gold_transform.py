import os
import sys
sys.path.append(r"D:\pharma_pipeline")

from config.spark_session import get_spark_session
from config.settings import SILVER_DIR, GOLD_DIR
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window

def transform_to_gold(spark: SparkSession) -> None:
    silver_path = os.path.join(SILVER_DIR, "pharma_sales")

    print(f"Reading silver data from : {silver_path}")
    df = spark.read.parquet(silver_path)
    print(f"Total records in Silver : {df.count()}")

    df_product_monthly = df.groupBy(
        "sale_year",
        "sale_month",
        "category",
        "product",
        "subject_area"
    ).agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("units_sold").alias("total_units"),
        F.count("*").alias("record_count"),
        F.avg("avg_unit_price").alias("avg_unit_price")
    ).orderBy("sale_year", "sale_month", "product")

    df_team_monthly = df.groupBy(
        "sale_year",
        "sale_month",
        "sales_team",
        "category"
    ).agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("units_sold").alias("total_units"),
        F.count("*").alias("record_count"),
        F.avg("avg_unit_price").alias("avg_unit_price")
    ).orderBy("sale_year", "sale_month", "sales_team")

    df_product_monthly = df_product_monthly.withColumn(
    "total_revenue", F.round("total_revenue", 2)
    ).withColumn(
    "avg_unit_price", F.round("avg_unit_price", 2)
    )

    wind_spec = Window.partitionBy("sale_year","sale_month").orderBy(F.desc("total_revenue"))
    lag_spec = Window.partitionBy("product","subject_area").orderBy("sale_year","sale_month")

    df_product_monthly = df_product_monthly\
        .withColumn("revenue_rank", F.rank().over(wind_spec))\
        .withColumn("prev_revenue", F.lag("total_revenue",1).over(lag_spec))\
        .withColumn("revenue_change", F.round((F.col("total_revenue") - F.col("prev_revenue"))/F.col("prev_revenue")*100, 2))

    df_team_monthly = df_team_monthly.withColumn(
    "total_revenue", F.round("total_revenue", 2)
    ).withColumn(
    "avg_unit_price", F.round("avg_unit_price", 2)
    )

    product_path = os.path.join(GOLD_DIR, "product_monthly")
    team_path = os.path.join(GOLD_DIR, "team_monthly")

    df_product_monthly.write\
    .mode("overwrite")\
    .partitionBy("category")\
    .parquet(product_path)

    df_team_monthly.write \
    .mode("overwrite") \
    .partitionBy("category") \
    .parquet(team_path)

    print(f"Gold product_monthly written to: {product_path}")
    print(f"Gold team_monthly written to: {team_path}")

    print("\n---Product Monthly Sample---")
    df_product_monthly.show(5, truncate=False)

    print("\n---Team Monthly Sample---")
    df_team_monthly.show(5, truncate=False)

if __name__ == "__main__":
    spark = get_spark_session("Gold_Transform")
    transform_to_gold(spark)
    spark.stop()