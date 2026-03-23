from pyspark.sql import SparkSession
import os 
import sys 
from LOG_MESSAGE import ERROR 

GCP_BUCKET = sys.argv[1]
if GCP_BUCKET[:5] != "gs://" : 
    raise ValueError(ERROR + "le bucket cloud devrait commencer par gs://")
wet_parquet = "wet_parquet"
wet_parquet_path = f"{GCP_BUCKET}/{wet_parquet}/"
spark_session = SparkSession.builder \
    .getOrCreate()
#df = spark.read.option("recursiveFileLookup", "true").parquet("wat_parquet/")
df_wet = spark_session.read \
    .option("recursiveFileLookup", "true") \
    .option("mergeSchema", "true") \
    .parquet(wet_parquet_path)

df_wet.show(10, truncate=False)