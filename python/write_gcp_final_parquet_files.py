from pyspark.sql import SparkSession
import os 
from LOG_MESSAGE import INFO, DEBUG, ERROR
import sys 

bucket_path = sys.argv[1]
if bucket_path[:5] != "gs://":
    raise ValueError(ERROR + "le bucket devrait commencer par gs://")
wet_parquet = bucket_path + "/wet_parquet"
wat_parquet = bucket_path + "/wat_parquet"

spark_session = SparkSession.builder \
    .getOrCreate()

first_url = int(sys.argv[2])
last_url = int(sys.argv[3])

url = str(first_url) + "_" + str(last_url)
#df = spark.read.option("recursiveFileLookup", "true").parquet("wat_parquet/")
wet_parquet = f"{bucket_path}/wet_parquet/wet_parquet_files_{url}"
wat_parquet = f"{bucket_path}/wat_parquet/wat_parquet_files_{url}"

df_wet = spark_session.read.parquet(wet_parquet)

df_wat = spark_session.read.parquet(wat_parquet)

df_wet = df_wet.drop("warc_id")
df_wat = df_wat.drop("warc_id")

df_joined = df_wet.join(df_wat, on="WARC_REFERS_TO", how="inner")

df_joined.write.mode("overwrite").parquet(f"{bucket_path}/final_parquet/final_parquet_files_{url}.parquet")
print(INFO + f"final parquet écrit pour url = {url}")

df_joined.show(10)