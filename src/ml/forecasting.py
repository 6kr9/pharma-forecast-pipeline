import os
import sys
sys.path.append(r"D:\pharma_pipeline")

import pandas as pd
from prophet import Prophet
import warnings
warnings.filterwarnings("ignore")
from pyspark.sql import SparkSession

from config.spark_session import get_spark_session
from config.settings import GOLD_DIR,OUTPUT_DIR

def prepare_prophet_df(spark_df, product_name: str)->pd.DataFrame:
    #filter for single product, prophet needs 2 cols-> date and value
    from pyspark.sql import functions as F

    df = spark_df.filter(F.col("product")==product_name)

    df = df.withColumn("ds", F.to_date(
        F.concat(
            F.col("sale_year").cast("string"),
            F.lit("-"),
            F.col("sale_month").cast("string"),
            F.lit("-01")
        ),"yyyy-M-dd"
        )
    )

    df = df.groupBy("ds").agg(
        F.sum("total_revenue").alias("y")
    ).orderBy("ds")

    rows = df.collect()
    return pd.DataFrame([(row["ds"], row["y"]) for row in rows], 
                       columns=["ds", "y"])

def train_and_forecast(pdf:pd.DataFrame, periods: int=6)->pd.DataFrame:

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        interval_width=0.95
    )

    model.fit(pdf)

    future = model.make_future_dataframe(periods=periods, freq = 'MS')

    forecast = model.predict(future)

    result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    result.columns = ["date", "forecasted_revenue", "lower_bound", "upper_bound"]
    result["forecasted_revenue"] = result["forecasted_revenue"].round(2)
    result["lower_bound"] = result["lower_bound"].round(2)
    result["upper_bound"] = result["upper_bound"].round(2)

    return result

def run_forecasting()->None:
    spark = get_spark_session("ML_Forecasting")
    product_path = os.path.join(GOLD_DIR, "product_monthly")
    df = spark.read.parquet(product_path)
    products = [row["product"] for row in df.select("product").distinct().collect()]
    print(f"Forecasting for {len(products)} products:{products}")

    all_forecasts = []

    for product in products:
        print(f"\nTraining model for :{product}")

        pdf = prepare_prophet_df(df, product)
        print(f"Training data points: {len(pdf)}")

        forecast_df = train_and_forecast(pdf, periods=6)

        forecast_df["product"] = product

        all_forecasts.append(forecast_df)

    final_df = pd.concat(all_forecasts, ignore_index=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "sales_forecast.csv")
    final_df.to_csv(output_path, index=False)

    print(f"\nForecast saved to: {output_path}")
    print(f"Total rows: {len(final_df)}")
    print("\nSample:")
    print(final_df[final_df["product"] == products[0]].tail(8).to_string())

    spark.stop()

if __name__ == "__main__":
    run_forecasting()
