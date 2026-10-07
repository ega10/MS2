from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import Tuple
def load_circulation_data(spark:SparkSession,path:str)->DataFrame:
    df=spark.read.csv(path,header=True,inferSchema=True)
    df=df.withColumn("borrow_date",to_date(col("borrow_date")))
    df=df.withColumn("return_date",to_date(col("return_date")))
    df=df.select("book_id","member_id","borrow_date","return_date","category")
    return df
def with_late_days(df:DataFrame)->DataFrame:
    df=df.withColumn("days",date_diff(col("return_date"),col("borrow_date")))
    return df
def filter_valid_loans(df:DataFrame)->DataFrame:
    df=df.filter(col("days")>=0)
    return df
def category_popularity(df:DataFrame)->DataFrame:
    df=(df.groupBy("category")
        .agg(count("*").alias("borrows"))
    )
    return df
def top_category(df:DataFrame)->Tuple[str,int]:
    df=(df.orderBy(col("borrows").desc(),
                  col("category").asc())
            .limit(1)
            .collect()
    )
    return(df[0]["category"],df[0]["borrows"])
    