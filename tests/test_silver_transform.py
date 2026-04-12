import sys
sys.path.append(r"D:\pharma_pipeline")

import pytest
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType
)


@pytest.fixture(scope="session")
def spark():
    #single spark session for all tests
    return SparkSession.builder \
    .appName("TestSilverTransform") \
    .master("local[1]") \
    .getOrCreate()

def make_test_df(spark, rows):
    #test df for silver
    schema = StructType([
        StructField("record_id", StringType(), True),
        StructField("sale_date",    StringType(),  True),
        StructField("product",      StringType(),  True),
        StructField("category",     StringType(),  True),
        StructField("subject_area", StringType(),  True),
        StructField("sales_team",   StringType(),  True),
        StructField("zip_code",     StringType(),  True),
        StructField("account_id",   StringType(),  True),
        StructField("units_sold",   IntegerType(), True),
        StructField("sales_amount", DoubleType(),  True),
        StructField("revenue",      DoubleType(),  True),
        StructField("region",       StringType(),  True),
        StructField("rep_id",       StringType(),  True),
    ])
    return spark.createDataFrame(rows, schema)

def test_null_record_id_dropped(spark):
    #records with null id should be dropped
    rows = [
         ("REC001", "2023-01-01", "EYLEA", "ANTIVEGF", "SYMPHONY",
         "MS_ZIP", "ZIP001", "ACC001", 10, 1000.0, 5000.0, "NY", "REP001"),
        (None, "2023-01-01", "AVONEX", "MS", "SYMPHONY",
         "MS_ZIP", "ZIP002", "ACC002", 5, 500.0, 2000.0, "CA", "REP002"),
    ]
    df = make_test_df(spark, rows)
    result = df.dropna(subset = ["record_id"])
    assert result.count() == 1

def test_duplicated_removed(spark):
    rows = [
        ("REC001", "2023-01-01", "EYLEA", "ANTIVEGF", "SYMPHONY",
         "MS_ZIP", "ZIP001", "ACC001", 10, 1000.0, 5000.0, "NY", "REP001"),
        ("REC001", "2023-01-01", "EYLEA", "ANTIVEGF", "SYMPHONY",
         "MS_ZIP", "ZIP001", "ACC001", 10, 1000.0, 5000.0, "NY", "REP001"),
        ("REC002", "2023-02-01", "AVONEX", "MS", "VALUECENTRIC",
         "MS_ACCOUNT", "ZIP002", "ACC002", 5, 500.0, 2000.0, "CA", "REP002"),
    ]
    df = make_test_df(spark, rows)
    result = df.dropDuplicates(["record_id"])
    assert result.count() == 2

def test_negative_revenue_removed(spark):
    #records with revenue <=0 should be filtered out
    rows = [
        ("REC001", "2023-01-01", "EYLEA", "ANTIVEGF", "SYMPHONY",
         "MS_ZIP", "ZIP001", "ACC001", 10, 1000.0, 5000.0, "NY", "REP001"),
        ("REC002", "2023-02-01", "AVONEX", "MS", "SYMPHONY",
         "MS_ZIP", "ZIP002", "ACC002", 5, 500.0, -100.0, "CA", "REP002"),
        ("REC003", "2023-03-01", "REBIF", "MS", "SYMPHONY",
         "MS_ZIP", "ZIP003", "ACC003", 3, 300.0, 0.0, "TX", "REP003"),
    ]
    df = make_test_df(spark, rows)
    result = df.filter(F.col("revenue")>0)
    assert result.count() == 1

def test_product_name_uppercased(spark):
    #prod names should be uppercased and trimmed

     rows = [
        ("REC001", "2023-01-01", "  eylea  ", "antivegf", "symphony",
         "ms_zip", "ZIP001", "ACC001", 10, 1000.0, 5000.0, "ny", "REP001"),
    ]
     df = make_test_df(spark, rows)
     result = df.withColumn("product", F.upper(F.trim(F.col("product"))))
     actual = result.collect()[0]["product"]
     assert actual == "EYLEA"

def test_avg_unit_price(spark):
    #avg_unit_price = revenue/units_sold
    rows = [
        ("REC001", "2023-01-01", "EYLEA", "ANTIVEGF", "SYMPHONY",
         "MS_ZIP", "ZIP001", "ACC001", 10, 1000.0, 5000.0, "NY", "REP001"),
    ]
    df = make_test_df(spark, rows)
    result = df.withColumn(
        "avg_unit_price",
        F.round(F.col("revenue") / F.col("units_sold"), 2)
    )
    actual = result.collect()[0]["avg_unit_price"]
    assert actual == 500.0