import os 
import sys
sys.path.append(r"D:\pharma_pipeline")

from config.spark_session import get_spark_session
from config.settings import BRONZE_DIR,SILVER_DIR
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def transform_to_silver(spark: SparkSession)-> None:
    bronze_path = os.path.join(BRONZE_DIR, "pharma_sales")
    silver_path = os.path.join(SILVER_DIR,"pharma_sales")

    print(f"Reading bronze data from : {bronze_path}")

    df = spark.read.parquet(bronze_path)

    print(f"Records before cleaning: {df.count()}")

    df = df.withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))

    critical_cols = ["record_id", "sale_date", "product", "category", "revenue"]
    df = df.dropna(subset=critical_cols)

    df = df.fillna({
        "region" : "UNKNOWN",
        "rep_id" : "UNKNOWN",
        "subject_area" : "UNKNOWN",
        "sales_team" : "UNKNOWN",
    })

    df = df.dropDuplicates(["record_id"])
    df = df.filter(F.col("revenue") > 0)
    df = df.filter(F.col("units_sold") > 0)
    df = df.filter(F.col("sale_date") <= F.current_date())

    df = df.withColumn("product", F.upper(F.trim(F.col("product"))))\
           .withColumn("category", F.upper(F.trim(F.col("category"))))\
           .withColumn("subject_area", F.upper(F.trim(F.col("subject_area"))))\
           .withColumn("sales_team", F.upper(F.trim(F.col("sales_team"))))\
           .withColumn("region", F.upper(F.trim(F.col("region"))))
    
    df = df.withColumn("sale_year", F.year("sale_date"))\
           .withColumn("sale_month", F.month("sale_date"))\
           .withColumn("sale_quarter", F.quarter("sale_date"))\
           .withColumn("avg_unit_price", F.round(F.col("revenue")/F.col("units_sold"), 2))
    
    df = df.withColumn("silver_processed_at", F.current_timestamp())

    print(f"Records after cleaning:{df.count()}")

    df.write\
      .mode("overwrite")\
      .partitionBy("category", "sale_year")\
      .parquet(silver_path)
    
    print(f"Silver layer written to : {silver_path}")
    print("\nSchema:")
    df.printSchema()
    print(f"partitions:{df.rdd.getNumPartitions()}")
    print("\nSample:")
    df.show(3, truncate=False)

if __name__== "__main__":
    spark = get_spark_session("Silver_Transform")
    transform_to_silver(spark)
    spark.stop()