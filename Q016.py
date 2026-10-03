# pyspark.errors.exceptions.base.PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got float.
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def load_kyc_data(spark: SparkSession, path: str) -> DataFrame:
    df=(spark.read.csv(path,header=True,inferSchema=True)
        .withColumn("onboarding_date",to_date(col("onboarding_date")))
    )
    return df

def remove_invalid_emails(df: DataFrame) -> DataFrame:
    df=df.filter(col("email").isNotNull())
    df=df.withColumn("email",(trim("email")))
    df=df.filter(col("email")!="")
    df=df.filter(col("email").rlike(r"^.+@.+\..+$"))
    return df

def filter_review_customers(df: DataFrame, min_score: int, max_score: int) -> DataFrame:
    df=df.filter(col("risk_score").between(min_score,max_score))
    df=df.filter(col("kyc_status").isin("PENDING","REVIEW"))
    return df

def risk_score_statistics(df: DataFrame) -> dict:
    df=(
        df.filter(col("risk_score").isNotNull())
        .agg(
            min("risk_score").alias("min_score"),
            max("risk_score").alias("max_score"),
            avg("risk_score").alias("avg_score"),
            count("risk_score").alias("total_customers"))
            .collect()
    )
    return {
        "min_score":df[0]["min_score"],
        "max_score":df[0]["max_score"],
        "avg_score":df[0]["avg_score"],
        "total_customers":df[0]["total_customers"]
    }

def city_highest_average_risk(df: DataFrame) -> tuple:
    df=(df.filter(
        col("city").isNotNull()&
        col("risk_score").isNotNull())
        .groupBy("city")
        .agg(avg("risk_score").alias("average_risk"))
        .orderBy(col("average_risk").desc(),
                 col("city").asc())
        .limit(1)
        .collect()
    )
    if not df:
        return("",0.0)
    return(df[0]["city"],float(df[0]["average_risk"]))

