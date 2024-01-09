import os
from pyspark.sql import SparkSession, Row,DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,DoubleType,TimestampType
from pyspark.sql.window import Window
from sentence_transformers import SentenceTransformer
import sklearn
# from pyspark.ml.feature import Normalizer
import pandas as pd
import numpy as np
KAFKA_ADDRESS=os.environ['REDPANDA_ADDR']
SPARK_MASTER_ADDRESS=os.environ["SPARK_ADDR"]
POSTGRES_DB=os.environ["POSTGRES_DB"]
POSTGRES_USER=os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD=os.environ["POSTGRES_PASSWORD"]
POSTGRES_PASSWORD=os.environ["POSTGRES_PASSWORD"]
CUTOFF_NUM=os.environ.get("CUTOFF_NUM",10)

spark:SparkSession = SparkSession.builder.master("spark://"+SPARK_MASTER_ADDRESS).getOrCreate()
spark.sparkContext.setCheckpointDir("hdfs://namenode:8020/checkpoints")
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
try:
    model=SentenceTransformer("/model/lastest.pth")
except:
    model = SentenceTransformer("all-MiniLM-L6-v2")
corpus_src=spark.read.format("jdbc").options(
    **{
        "url":f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}",
        "query": "select review,movie from review_data WHERE review is NOT NULL",
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }
).load()
@F.pandas_udf(returnType=ArrayType(DoubleType()))
def encode(x: pd.Series) -> pd.Series:
    return pd.Series(sklearn.preprocessing.Normalizer().fit_transform(model.encode(x)).tolist())
# @F.pandas_udf(returnType=ArrayType(DoubleType()))
# def normalize(emb:pd.Series) -> pd.Series:
#     emb=np.array(emb.to_list())
#     # raise Exception(f"{emb}\n{emb.shape}\n{type(emb)}")
#     norm=np.linalg.norm(np.array(emb),axis=-1)
#     return pd.Series(np.einsum("ij,i->i",emb,1/norm))
corpus=corpus_src.withColumn("review",F.col("review").cast(StringType()))\
    .select("movie",encode("review").alias("embedding"))\
    .withColumn("embedding",F.slice("embedding",1,384))
corpus.show()

@F.pandas_udf(returnType=ArrayType(DoubleType()))
def dot_fun(x:pd.Series,y:pd.Series)->pd.Series:
    def seriesTo2D(s):
        return np.array(s.values.tolist())
    r=sklearn.metrics.pairwise.linear_kernel(seriesTo2D(x),seriesTo2D(y))
    return pd.Series(r.tolist())
df=spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers",KAFKA_ADDRESS)\
    .option("subscribe", "inference-input")\
    .load()
# df.writeStream.outputMode("append").format("console").start()
parsed_df=df.withColumn("value",F.col("value").cast("string"))\
    .select("key","timestamp",encode("value").alias("value"))\
    .withColumn("value",F.slice("value",1,384))
_scores=parsed_df.crossJoin(corpus.select('movie', "embedding"))
scores=_scores.withColumn("tmp",dot_fun("value","embedding")).select("key","timestamp","movie",F.col("tmp").alias("score"))
# grouped=scores\
#     .withColumn("timestamp",F.current_timestamp())\
#     .withWatermark("timestamp", "20 minutes")\
#     .groupBy("timestamp","key").agg(F.collect_list(F.struct("score", "movie")).alias("tmp"))
# prefinal=grouped.select("key","timestamp", F.sort_array("tmp")["movie"].alias("movie"))\
scores.printSchema()
grouped=scores.groupBy("key").agg(F.collect_list(F.struct("score", "movie")).alias("tmp"))
prefinal=grouped.select("key", F.sort_array("tmp")["movie"].alias("movie"))\
    .withColumn("value",F.array([F.col("movie")[i] for i in range(CUTOFF_NUM)]))
final=prefinal.withColumn("value",F.to_json("value"))\
    .withColumn("value",F.to_binary("value",F.lit("utf-8")).alias('r'))\
    .select("key","value")
final.writeStream.outputMode("complete").format("console").start()
final.writeStream.outputMode("complete").format("kafka")\
    .option("kafka.bootstrap.servers",KAFKA_ADDRESS)\
    .option("topic", "inference-output")\
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints")\
    .start()


# df.select((F.col("value").cast("string")).alias("review"))\
#     .withColumn("embedding",normalizer.transform(encode("review")))
# reviews_binarykey=df.select(F.col("key"))
# reviews_mat = IndexedRowMatrix(
#     reviews.select("movie", "embedding")\
#         .rdd.map(lambda row: IndexedRow(row.movie, row.embedding.toArray())))
# dot = reviews_mat.multiply(corpus_mat.transpose())
# scores=dot.toBlockMatrix().toLocalMatrix().toArray()
# np.split(scores,nrow)
# scoresdf=spark.createDataFrame(scores)
# scoresdf
print("All set")
# reviews.writeStream.outputMode("append").format("console").start()
# query = reviews.writeStream.foreachBatch(writeToExternal).start()

# reviews_processed.writeStream.outputMode("append").format("console").start()

while len(spark.streams.active) > 0:
  spark.streams.resetTerminated()
  spark.streams.awaitAnyTermination()
