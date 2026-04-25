import os
import sys
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "PharmaForecast") -> SparkSession:
    if sys.platform == "win32":
        os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
        os.environ["SPARK_HOME"] = r"C:\spark\spark-4.1.1-bin-hadoop3"
        os.environ["PATH"] = r"C:\Program Files\Java\jdk-17\bin;" + os.environ["PATH"]
        os.environ["PATH"] = r"C:\spark\spark-4.1.1-bin-hadoop3\bin;" + os.environ["PATH"]

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir",
                r"D:\pharma_pipeline\data" if sys.platform == "win32"
                else "/mnt/d/pharma_pipeline/data") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark