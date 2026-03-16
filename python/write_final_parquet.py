from pyspark.sql import SparkSession
import os 
from LOG_MESSAGE import INFO, DEBUG, ERROR

wet_parquet = os.path.abspath("wet_parquet")
wat_parquet = os.path.abspath("wat_parquet")

spark_session = SparkSession.builder \
    .getOrCreate()

pas = 100000
for i in range(100000,900000,pas):
    url = str(i) + "_" + str(i+pas)
#df = spark.read.option("recursiveFileLookup", "true").parquet("wat_parquet/")
    wet_parquet = f"wet_parquet/wet_parquet_files_{url}.parquet"
    wat_parquet = f"wat_parquet/wat_parquet_files_{url}.parquet"

    df_wet = spark_session.read.parquet(wet_parquet)

    df_wat = spark_session.read.parquet(wat_parquet)

    df_wet = df_wet.drop("warc_id")
    df_wat = df_wat.drop("warc_id")

    df_joined = df_wet.join(df_wat, on="WARC_REFERS_TO", how="inner")

    df_joined.write.mode("overwrite").parquet(f"final_parquet/final_parquet_files_{url}.parquet")
    print(INFO + f"final parquet écrit pour url = {url}")

df_joined.show(10)