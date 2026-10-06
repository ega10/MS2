from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def load_ad_impressions(spark: SparkSession, path: str) -> DataFrame:
    df=(spark.read.csv(path,header=True,inferSchema=True)
        .withColumn("impression_ts",col("impression_ts").cast("TimeStamp"))
        .withColumn("watched_seconds",col("watched_seconds").cast("Double"))
        .withColumn("ad_length_seconds",col("ad_length_seconds").cast("Double"))
        .withColumn("spend_amount",col("spend_amount").cast("Double"))
    )
    return df
def join_campaign_metadata(impressions_df: DataFrame, campaigns_df: DataFrame) -> DataFrame:
    df=impressions_df.join(campaigns_df,on="campaign_id",how="inner")
    return df
def add_engagement_metrics(df: DataFrame) -> DataFrame:
    df=df.withColumn("watch_pct",when(
        ((col("ad_length_seconds").isNull())|
        (col("ad_length_seconds")<=0)),lit(0.0)
    )
    .otherwise((col("watched_seconds")/
               col("ad_length_seconds"))*100
               ))
    df=df.withColumn("engagement_band",when(
        col("watch_pct")>=80,lit("High")
    )
    .when(col("watch_pct")>=40,lit("Medium")
          )
    .otherwise(lit("Low")))
    return df

def campaign_performance_summary(df: DataFrame) -> DataFrame:
    df=(df.filter(col("delivery_status")=="DELIVERED")
        .groupBy("campaign_id","campaign_name")
        .agg(count("impression_id").alias("impression_count"),
             sum("spend_amount").alias("total_spend"),
             sum(when(
                 col("clicked")=="Y",lit(1)
             )
             .otherwise(lit(0))
             ).alias("click_count")
        ))
    return df
    
def top_n_campaigns_by_spend(df: DataFrame, n: int) -> DataFrame:
    df=(df.orderBy(col("total_spend").desc(),
                  col("campaign_id").asc())
            .limit(n)
    )
    return df
    

