# test_03_enrich_settlements - pyspark.errors.exceptions.base.PySparkValueError: [CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def define_settlement_schema() -> StructType:
    schema=StructType([
        StructField("settlement_id",StringType(),True),
        StructField("merchant_id",StringType(),True),
        StructField("settlement_ts",StringType(),True),
        StructField("gross_amount",DoubleType(),True),
        StructField("fee_amount",DoubleType(),True),
        StructField("settlement_status",StringType(),True)
    ])
    return schema

def load_settlement_data(spark: SparkSession, settlements_path: str, merchants_path: str, schema: StructType) -> tuple:
    df1=spark.read.csv(settlements_path,header=True,schema=schema)
    df2=spark.read.csv(merchants_path,header=True,inferSchema=True)
    df1=df1.withColumn("settlement_ts",col("settlement_ts").cast("TimeStamp"))
    df2=df2.withColumn("risk_score",col("risk_score").cast("int"))
    return (df1,df2)

def enrich_settlements(settlements_df: DataFrame, merchants_df: DataFrame) -> DataFrame:
    df=(settlements_df.join(merchants_df,on="merchant_id",how="inner")
        .withColumn("city",coalesce(col("city"),lit("Unknown")))
        .withColumn("merchant_label",concat_ws(" - ",col("merchant_name"),col("city")))
        .withColumn("net_amount",col("gross_amount")-col("fee_amount"))
    )
    return df
def merchants_without_successful_settlement(merchants_df: DataFrame, settlements_df: DataFrame) -> DataFrame:
    df1=(settlements_df
        .filter(col("settlement_status")=="SUCCESS")
        .select("merchant_id")
        .distinct()
    )
    df=merchants_df.join(df1,on="merchant_id",how="left_anti")
    return df

def rank_merchants_by_net_amount(df: DataFrame) -> DataFrame:
    df=(df.filter(col("settlement_status")=="SUCCESS")
        .groupBy("merchant_id","merchant_label")
        .agg(sum("net_amount").alias("total_net_amount"),
        count("settlement_id").alias("settlement_count"))
    )
    w=Window.orderBy(col("total_net_amount").desc())
    df=df.withColumn("settlement_rank",rank().over(w))
    return df


