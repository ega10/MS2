from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

def load_battery_swap_data(spark: SparkSession, path: str) -> DataFrame:
    df=(spark.read.csv(path,header=True,inferSchema=True)
        .withColumn("swap_date",to_date("swap_date"))
    )
    return df
def add_swap_month(df: DataFrame) -> DataFrame:
    df=df.withColumn("swap_month",date_trunc("month",col("swap_date")))
    return df
def filter_valid_swaps(df: DataFrame) -> DataFrame:
    df=df.filter(
        ((col("energy_kwh")>=0)&
        (col("battery_type").isNotNull()))
        )
    return df

def monthly_battery_energy(df: DataFrame) -> DataFrame:
    df=(df.groupBy("battery_type","swap_month")
        .agg(sum("energy_kwh").alias("total_energy_kwh"))
    )
    return df
def top_battery_type(df: DataFrame) -> Tuple[str, float]:
    df=(df.groupBy("battery_type")
        .agg(sum("total_energy_kwh").alias("total_energy"))
        .orderBy(col("total_energy").desc(),
                 col("battery_type").asc())
        .limit(1)
        .collect()
    )
    if not df:
        return("",0.0)
    return(df[0]["battery_type"],float(df[0]["total_energy"]))
