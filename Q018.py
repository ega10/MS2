# All pass
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def load_vaccine_batches(spark: SparkSession, path: str) -> DataFrame:
    df=(spark.read.csv(path,header=True,inferSchema=True)
        .withColumn("manufacture_date",to_date(col("manufacture_date")))
        .withColumn("expiry_date",to_date(col("expiry_date")))
    )
    return df
def drop_incomplete_batches(df: DataFrame) -> DataFrame:
    df=df.dropna(subset=["batch_id","product_name","manufacture_date","expiry_date","potency_pct"])
    return df
def add_stability_dates(df: DataFrame) -> DataFrame:
    df=df.withColumn("shelf_life_days",datediff(col("expiry_date"),col("manufacture_date")))
    df=df.withColumn("review_date",date_add(col("manufacture_date"),30))
    df=df.withColumn("days_since_manufacture",datediff(current_date(),col("manufacture_date")))
    return df
def filter_potency_range(df: DataFrame, low: float, high: float) -> DataFrame:
    df=df.filter(col("potency_pct").between(low,high))
    return df

def count_release_ready_batches(df: DataFrame) -> int:
    df=(df.filter(
        (col("release_status")=="RELEASED")
        &(col("potency_pct").isNotNull())
    )
        .count()
    )
    return df
