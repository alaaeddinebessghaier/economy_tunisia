from pyspark import *
from utils.spark_session import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json , col , sum


spark = get_spark_session("jobs_repos")

metadata_schema = StructType([
    StructField("source", StringType()),
    StructField("pipeline", StringType()),
    StructField("fetched_at", StringType()),
    StructField("environment",StringType())
])

schema = StructType([
    StructField("source",StringType()),
    StructField("title",StringType()),
    StructField("description",StringType()),
    StructField("url",StringType()),
    StructField("content",DoubleType()),
    StructField("metadata",metadata_schema)
])


df_rp = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "news_repos") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()


df_parse = df_rp \
    .selectExpr("CAST(value AS STRING) as value") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select(
        col("data.source"),
        col("data.title"),
        col("data.description"),
        col("data.url"),
        col("data.content"),

        col("data.metadata.source").alias("metadata_source"),
        col("data.metadata.pipeline").alias("pipeline"),
        col("data.metadata.fetched_at").alias("fetched_at"),
        col("data.metadata.environment").alias("environment"),
    )

df_parse.printSchema()
df_parse.show(5,truncate=False)

df_parse.select([sum(col(c).isNull().cast('int')).alias(c) for c in df_parse.columns]).show()
print("numbers of rows",df_parse.count())


df_clean = df_parse.drop('content','source')
df_clean.select([sum(col(c).isNull().cast('int')).alias(c) for c in df_clean.columns]).show()


df_clean.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/economy_db") \
    .option("dbtable", "news_repos") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()