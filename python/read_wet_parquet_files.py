from pyspark.sql import SparkSession
import os 

wet_parquet = os.path.abspath("wet_parquet")
wat_parquet = os.path.abspath("wat_parquet")
final_parquet = os.path.abspath("final_parquet")
spark_session = SparkSession.builder \
    .getOrCreate()
#df = spark.read.option("recursiveFileLookup", "true").parquet("wat_parquet/")
df_wet = spark_session.read.option("recursiveFileLookup", "true") \
               .option("mergeSchema", "true") \
               .parquet(wet_parquet)


print(df_wet.count())