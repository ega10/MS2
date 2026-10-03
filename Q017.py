from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def load_inline_triage_data(spark: SparkSession) -> DataFrame:
    data=[
        ("C001","ER","2026-09-01 08:00:00","2026-09-01 08:25:00","High","ACTIVE"),
        ("C002","Trauma","2026-09-01 08:10:00","2026-09-01 08:05:00","Critical","ACTIVE"),
        ("C003","Pediatrics","2026-09-01 09:00:00","2026-09-01 09:45:00","Low","ACTIVE"),
        ("C004","ER","2026-09-01 09:30:00","2026-09-01 10:00:00","High","CLOSED")
    ]
    return spark.createDataFrame(data,
                                 ["case_id","department","arrival_time","doctor_start_time","priority","status"])

def compute_wait_minutes(df: DataFrame) -> DataFrame:
    wait_minutes=(
        (
            unix_timestamp(col("doctor_start_time"))-
            unix_timestamp(col("arrival_time"))
        )/60
        )
    df=df.withColumn("wait_minutes",when(
        (wait_minutes <0),0
    )
    .otherwise(wait_minutes))
    return df

def filter_priority_cases(df: DataFrame) -> DataFrame:
    df=(df.filter(
        col("priority").isin("Critical","High")&
        (col("wait_minutes").isNotNull())
        )
    )
    return df 
def average_wait_by_department(df: DataFrame) -> DataFrame:
    df=(df.groupBy("department")
        .agg(avg("wait_minutes").alias("avg_wait_minutes"))
    )
    return df

def list_active_departments(df: DataFrame) -> list:
    df=(df.filter(col("status")=="ACTIVE")
        .select("department")
        .distinct()
        .orderBy(col("department").asc())
        .collect()
    )
    lst=[]
    for row in df:
        lst.append(row["department"])
    return lst

