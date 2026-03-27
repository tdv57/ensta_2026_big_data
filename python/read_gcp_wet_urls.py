from pyspark.sql import SparkSession
import os 
import download_wet as dwet
import sys 
from LOG_MESSAGE import ERROR

bucket_path = sys.argv[1]
if bucket_path[:5] != "gs://":
    raise ValueError(ERROR + "le bucket cloud devrait commencer par gs://")
spark_session = SparkSession.builder \
    .getOrCreate()
first_url = int(sys.argv[2])
last_url = int(sys.argv[3])
pas = int(sys.argv[4])
df_urls = dwet.gcp_build_df_urls(
    spark_session=spark_session,
    bucket_path=bucket_path,
    first_url=first_url,
    last_url=last_url,
    pas=pas
)

print(f"[INFO] df_urls count = {df_urls.count()}")
df_urls.show(10, truncate=False)