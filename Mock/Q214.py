from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
def load_irrigation_data(spark:SparkSession,path:str)->DataFrame:
    df=(spark.read.csv(path,header=True,inferSchema=True)
        .withColumn("irrigation_date",to_date(col("irrigation_date")))
    )
    return df
def with_month(df:DataFrame)->DataFrame:
    df=df.withColumn("month",date_trunc("month",col("irrigation_date")))
    df=df.withColumn("month",to_date(col("month")))
    return df
def filter_valid(df:DataFrame)->DataFrame:
    df=df.filter(col("liters_used")>=0)
    return df
def monthly_crop_water(df:DataFrame)->DataFrame:
    df=(df.groupBy("crop_type","month")
        .agg(sum("liters_used").alias("total_liters"))
    )
    return df
def top_crop_by_month(df:DataFrame)->str:
    df=(df.groupBy("crop_type")
        .agg(sum("total_liters").alias("total_liters"))
        .orderBy(col("total_liters").desc())
        .limit(1)
        .collect()
    )
    return df[0]["crop_type"]
