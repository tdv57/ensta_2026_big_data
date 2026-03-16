from pyspark.sql import SparkSession
import os 

wet_parquet = os.path.abspath("wet_parquet")
wat_parquet = os.path.abspath("wat_parquet")

spark_session = SparkSession.builder \
    .getOrCreate()
#df = spark.read.option("recursiveFileLookup", "true").parquet("wat_parquet/")
wet_parquet = "wet_parquet/wet_parquet_files_0_100000.parquet"
wat_parquet = "wat_parquet/wat_parquet_files_0_100000.parquet"

df_wet = spark_session.read.parquet(wet_parquet)

df_wat = spark_session.read.parquet(wat_parquet)

for i in range(100):
    first_row = df_wet.limit(100).collect()[i]
    warc_id = first_row['WARC_REFERS_TO']
    print(f"WARC_ID sélectionné : {warc_id}")
    matching_row = df_wat.filter(df_wat["WARC_REFERS_TO"] == warc_id).collect()
    if matching_row:
        print("Ligne correspondante dans df_wat :")
        print(matching_row[0])
    else:
        print("Aucune ligne correspondante trouvée dans df_wat.")