from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import Tuple
def load_rides_csv(spark:SparkSession,path:str)->DataFrame:
    df=spark.read.csv(path,header=True,inferSchema=True)
    return df
def find_loyal_customers(df:DataFrame,n:int)->DataFrame:
    df=(df.groupBy("customerID")
        .agg(count("RideId").alias("number_rides"))
        .filter(col("number_rides")>=n)
        .select("CustomerID")
    )
    return df
def apply_discounts(df:DataFrame,loyal_customers:DataFrame)->DataFrame:
    df1=loyal_customers.withColumn("is_loyal",lit(True))
    df=df.join(df1,on="CustomerID",how="left")
    df=df.withColumn("discount",when(
        col("is_loyal")==True,0.10
    )
    .otherwise(0.07))
    return df
def top_three_longest_trips(df:DataFrame)->DataFrame:
    df=(df.select("RideId","source","destination","distance")
        .orderBy(col("distance").desc())
        .limit(3)
    )
    return df
def get_top_earners(df:DataFrame)->DataFrame:
    df=(df.groupBy("DriverID")
        .agg(sum("earnings").alias("total_earnings"))
        .limit(3)
    )
    return df