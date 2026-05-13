from pyspark.sql.types import *
from utils.spark_session import get_spark_session
from pyspark.sql.functions import from_json, col , sum 
from pyspark.sql import functions as F


spark = get_spark_session("gituhb_repos")


metadata_schema = StructType([
    StructField("source",      StringType()),
    StructField("pipeline",    StringType()),
    StructField("query",       StringType()),
    StructField("fetched_at",  TimestampType()),
    StructField("environment", StringType()),
])

schema = StructType([
    StructField("id",                LongType()),
    StructField("full_name",         StringType()),
    StructField("language",          StringType()),
    StructField("stargazers_count",  IntegerType()),
    StructField("topics",            ArrayType(StringType())),
    StructField("metadata",          metadata_schema),
])


df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "github_repos") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()


df_parsed = df_raw \
    .selectExpr("CAST(value AS STRING) as value") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select(
        col("data.id"),
        col("data.full_name"),
        col("data.language"),
        col("data.stargazers_count"),
        col("data.topics"),
        col("data.metadata.source").alias("source"),
        col("data.metadata.pipeline").alias("pipeline"),
        col("data.metadata.query").alias("query"),
        col("data.metadata.fetched_at").alias("fetched_at"),
        col("data.metadata.environment").alias("environment"),
    )


#######EDA 


df_parsed.printSchema()
df_parsed.show(5, truncate=False)
df_parsed.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_parsed.columns]).show()
df_parsed.describe("stargazers_count").show()
print("total_records:",df_parsed.count())



#####CLEAN_DF

df_clean = df_parsed.select("id", "language", "stargazers_count", "topics", "fetched_at")
df_clean.show(5, truncate=False)
df_clean.groupBy("language").agg(
    F.sum("stargazers_count").alias("total_watch"),
    F.avg("stargazers_count").alias("average_watch")
).show()

df_clean = df_clean.dropna(subset=["id", "language", "stargazers_count"])
df_clean = df_clean.withColumnRenamed("stargazers_count", "starts")



df_clean.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/economy_db") \
    .option("dbtable", "github_repos") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()