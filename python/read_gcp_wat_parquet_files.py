from pyspark.sql import SparkSession
import os 
import sys 
from LOG_MESSAGE import ERROR 

GCP_BUCKET = sys.argv[1]
if GCP_BUCKET[:5] != "gs://" : 
    raise ValueError(ERROR + "le bucket cloud devrait commencer par gs://")
wat_parquet = "wat_parquet"
wat_parquet_path = f"{GCP_BUCKET}/{wat_parquet}/"
spark_session = SparkSession.builder \
    .getOrCreate()
#df = spark.read.option("recursiveFileLookup", "true").parquet("wat_parquet/")
df_wat = spark_session.read \
    .option("recursiveFileLookup", "true") \
    .option("mergeSchema", "true") \
    .parquet(wat_parquet_path)

df_wat.show(10, truncate=False)