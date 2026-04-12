import sys
from src.ingestion.generate_data import generate_sales_data,save_raw_data
from src.ingestion.bronze_ingestion import ingest_to_bronze
from src.transforms.silver_transform import transform_to_silver
from src.transforms.gold_transform import transform_to_gold
from src.ml.forecasting import run_forecasting

from config.spark_session import get_spark_session

def run():

    spark = get_spark_session()
    
    try:
        print("\n── Step 1: Generating raw data ──")
        try:
            df = generate_sales_data(num_records=10000)
            save_raw_data(df)
        except Exception as e:
            print(f"failed at generation: {e}")
            sys.exit(1)

        print("\n── Step 2: Bronze ingestion ──")
        try:
            ingest_to_bronze(spark)
        except Exception as e:
                print(f"failed at bronze : {e}")
                sys.exit(1)

        print("\n── Step 3: Silver transform ──")
        try:
            transform_to_silver(spark)
        except Exception as e:
            print(f"failed at Silver : {e}")
            sys.exit(1)

        print("\n── Step 3: Gold transform ──")
        try:
            transform_to_gold(spark)
        except Exception as e:
            print(f"failed at Gold : {e}")
            sys.exit(1)

        print("\n── Step 3: Forecasting ──")
        try:
            run_forecasting()
        except Exception as e:
            print(f"failed at forecasting : {e}")
            sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    run()