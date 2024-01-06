from typing import Dict,Callable
import os
from pyspark.sql import SparkSession, Row,DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,DoubleType
from sentence_transformers import SentenceTransformer
import pandas as pd
KAFKA_ADDRESS=os.environ['REDPANDA_ADDR']
SPARK_MASTER_ADDRESS=os.environ["SPARK_ADDR"]
# POSTGRES_ADDRESS=os.environ["POSTGRES_ADDR"]
# POSTGRES_USER=os.environ["POSTGRES_USER"]
# POSTGRES_PASSWORD=os.environ["POSTGRES_PASSWORD"]
os.makedirs("/tmp/spark/checkpoints",exist_ok=True)
spark:SparkSession = SparkSession.builder.master("spark://"+SPARK_MASTER_ADDRESS).getOrCreate()
fields={
    "critic":StringType(),
    "publication":StringType(),
    "state":StringType(),
    "review":StringType(),
    "date":StringType(),
    "grade":StringType(),
    "movie":StringType(),
}
schema=StructType([StructField(k,v) for k,v in fields.items()])
try:
    model=SentenceTransformer("/model/latest.pth")
except:
    model = SentenceTransformer("all-MiniLM-L6-v2")
# properties = {"user": POSTGRES_USER,"password": POSTGRES_PASSWORD,"driver": "org.postgresql.Driver"}
# def writeToExternal(df: DataFrame, batchId: int):
#     df.show()
    # df.write.jdbc(url=f"jdbc:postgresql://{POSTGRES_ADDRESS}",
    #                  table="test_result", 
    #                  mode="append", 
    #                  properties=properties)
print("All set")
df=spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers",KAFKA_ADDRESS)\
    .option("subscribe", "review")\
    .load()
reviews=df.select(F.from_json(F.col("value").cast("string"),schema).alias("parsed_value")).select(F.col("parsed_value.*"))
# reviews.writeStream.outputMode("append").format("console").start()
# query = reviews.writeStream.foreachBatch(writeToExternal).start()
@F.pandas_udf(returnType=ArrayType(DoubleType()))
def encode(x: pd.Series) -> pd.Series:
    return pd.Series(model.encode(x).tolist())
reviews_processed=reviews.withColumn("embedding", encode("review"))
reviews_processed.writeStream.outputMode("append").format("console").start()
# reviews_processed.writeStream.outputMode("append").format("kafka")\
#     .option("kafka.bootstrap.servers",KAFKA_ADDRESS)\
#     .option("topic", "reviews-processed")\
#     .option("checkpointLocation", "/tmp/spark/checkpoints")\
#     .start()
while len(spark.streams.active) > 0:
  spark.streams.resetTerminated()
  spark.streams.awaitAnyTermination()
