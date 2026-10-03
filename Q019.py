# All Pass
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def load_music_tracks(spark: SparkSession, path: str) -> DataFrame:
    df=spark.read.csv(path,header=True,inferSchema=True)
    df=df.withColumn("release_date",to_date(col("release_date")))
    return df

def fill_missing_genre(df: DataFrame) -> DataFrame:
    df=df.fillna({"genre":"Uncategorized"})
    return df

def add_release_calendar(df: DataFrame) -> DataFrame:
    df=df.withColumn("release_year",year(col("release_date")))
    df=df.withColumn("release_month",month(col("release_date")))
    df=df.withColumn("release_month_start",date_trunc("month",col("release_date")))
    return df
def unique_published_artists(df: DataFrame) -> list:
    df=df.filter(col("status")=="Published")
    df=(df.select(col("artist_name"))
        .distinct()
        .orderBy(col("artist_name").asc())
        .collect()
    )
    lst=[]
    for row in df:
        lst.append(row["artist_name"])
    return sorted(lst)
def top_n_genres_by_streams(df: DataFrame, n: int) -> DataFrame:
    df=(df.filter(col("status")=="Published")
        .groupBy("genre")
        .agg(sum("stream_count").alias("total_streams"))
        .orderBy(col("total_streams").desc(),
                 col("genre").asc())
        .limit(n)
    )
    return df
