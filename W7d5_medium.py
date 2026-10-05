#Q029
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

def load_delivery_trips(spark: SparkSession, path: str) -> DataFrame:
    df=(spark.read.csv(path,header=True,inferSchema=True)
        .withColumn("dispatch_ts",col("dispatch_ts").cast("TimeStamp"))
        .withColumn("promised_ts",col("promised_ts").cast("TimeStamp"))
        .withColumn("actual_ts",col("actual_ts").cast("TimeStamp"))
        .withColumn("distance_km",col("distance_km").cast("double"))
        .withColumn("fare_amount",col("fare_amount").cast("double"))
    )
    return df

def compute_trip_metrics(df: DataFrame) -> DataFrame:
    df=df.withColumn("delay_hours",
                     when(
                         (unix_timestamp("actual_ts")-
                          unix_timestamp("promised_ts"))/3600 >0,
                          (unix_timestamp("actual_ts")-
                                                    unix_timestamp("promised_ts"))/3600

                     ).otherwise(0.0))
    df=df.withColumn("trip_hours",
                     when(
                         (unix_timestamp("actual_ts")-unix_timestamp("dispatch_ts"))/3600 >0,
                         (unix_timestamp("actual_ts")-unix_timestamp("dispatch_ts"))/3600
                     )
                     .otherwise(0.0))
    df=df.withColumn("avg_speed_kmph",
                     when(
                         col("trip_hours")>0,
                         col("distance_km")/col("trip_hours")
                     )
                     .otherwise(0.0))
    return df

def filter_significant_delays(
    df: DataFrame,
    delay_threshold: float,
    min_distance: float
) -> DataFrame:
    df=df.filter(
        (col("delay_hours")>delay_threshold)&
        (col("distance_km")>=min_distance)
    )
    
    return df

def partner_delay_summary(df: DataFrame) -> DataFrame:
    df=(df.filter(col("status")=="Completed")
        .groupBy("partner")
        .agg(sum("delay_hours").alias("total_delay_hours"),
             avg("fare_amount").alias("avg_fare"),
             count("trip_id").alias("trip_count"))
    )
    return df

def highest_average_delay_reason(df: DataFrame) -> Tuple[str, float]:
    df=(df.filter(
        (col("delay_reason").isNotNull())&
        (col("delay_hours").isNotNull())
        )
        .groupBy("delay_reason")
        .agg(avg("delay_hours").alias("avg_delay_hours"))
        .orderBy(col("avg_delay_hours").desc(),
                 col("delay_reason").asc())
        .limit(1)
        .collect()
    )
    if not df:
        return("",0.0)
    return(df[0]["delay_reason"],float(df[0]["avg_delay_hours"]))
    
