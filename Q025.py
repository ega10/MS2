# Q025
#contains 1 error (got float)
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def define_observation_schema() -> StructType:
    schema=StructType([
        StructField("observation_id",StringType(),True),
        StructField("patient_id",StringType(),True),
        StructField("reading_ts",StringType(),True),
        StructField("systolic",StringType(),True),
        StructField("heart_rate",StringType(),True),
        StructField("device_status",StringType(),True)
            ])
    return schema

def load_monitoring_data(spark: SparkSession, observations_path: str, patients_path: str, schema: StructType) -> tuple:
    df1=(spark.read.csv(observations_path,header=True,schema=schema)
         .withColumn("reading_ts",col("reading_ts").cast("TimeStamp"))
         .withColumn("systolic",col("systolic").cast("int"))
         .withColumn("heart_rate",col("heart_rate").cast("int"))
    )
    df2=spark.read.csv(patients_path,header=True,inferSchema=True)
    return df1,df2
def classify_readings(df: DataFrame) -> DataFrame:
    df=df.withColumn("alert_level",
                     when(
                         (col("systolic")>=180)|
                         (col("heart_rate")>=130),lit("Critical")
                     )
                     .when(
                         (col("systolic")>=140)|
                         (col("heart_rate")>=100),lit("Warning")
                     )
                     .otherwise(lit("Normal"))
    )
    return df

def latest_reading_per_patient(df: DataFrame) -> DataFrame:
    win=(Window.partitionBy("patient_id")
         .orderBy(col("reading_ts").desc())
    )
    df=(df.withColumn("row_num",row_number().over(win))
        .filter(col("row_num")==1))
    return df

def care_team_alert_summary(observations_df: DataFrame, patients_df: DataFrame) -> DataFrame:
    df=(observations_df.join(patients_df,on="patient_id",how="inner")
        .filter(
        col("alert_level").isin("Critical","Warning")) 
        .groupBy("care_team")
        .agg(
            count(col("observation_id")).alias("alert_count"),
            avg(col("heart_rate")).alias("avg_heart_rate")
    )
    )
    return df


