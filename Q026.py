# test_04_join_reactor_metadata - pyspark.errors.exceptions.base.PySparkValueError: [CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring.
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def define_run_schema() -> StructType:
    schema=StructType([
        StructField("run_id",StringType(),True),
        StructField("reactor_id",StringType(),True),
        StructField("process_type",StringType(),True),
        StructField("start_ts",StringType(),True),
        StructField("end_ts",StringType(),True),
        StructField("input_mass",DoubleType(),True),
        StructField("output_mass",DoubleType(),True),
        StructField("run_status",StringType(),True),
    ])
    return schema

def load_bioreactor_data(spark: SparkSession, runs_path: str, reactors_path: str, schema: StructType) -> tuple:
    df=(spark.read.csv(runs_path,header=True,schema=schema)
        .withColumn("start_ts",col("start_ts").cast("TimeStamp"))
        .withColumn("end_ts",col("end_ts").cast("TimeStamp"))
    )
    df2=spark.read.csv(reactors_path,header=True,inferSchema=True)
    return df,df2
def compute_run_metrics(df: DataFrame) -> DataFrame:
    duration=(
            unix_timestamp(col("e"))-
            unix_timestamp(col("s")))/60
    df=df.withColumn("duration_minutes",
                        when(
                         duration <0,0)
                         .otherwise(duration))
    df=df.withColumn("yield_pct",
                        (col("output_mass")/col("input_mass"))*100)
    return df

def join_reactor_metadata(runs_df: DataFrame, reactors_df: DataFrame) -> DataFrame:
    df=(runs_df.join(
        reactors_df,on="reactor_id",how="inner"
    )
    .withColumn("facility",coalesce(col("facility"),lit("Unknown")))
    .withColumn("reactor_label",concat_ws(" - ",reactor_name,facility))
    )
    return df

def dense_rank_runs_by_yield(df: DataFrame) -> DataFrame:
    df=df.filter(
        (col("run_status")=="COMPLETED")&
        (col("yield_pct").isNotNull()))
    w=(Window.partitionBy("process_type")
       .orderBy(col("yield_pct").desc())
    )
    df=(df.withColumn("yield_rank",dense_rank().over(w))
         .select("run_id","process_type","yield_pct","yield_rank"))
    return df

