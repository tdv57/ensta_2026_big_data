from pyspark.sql import SparkSession

spark_session = SparkSession.builder.getOrCreate()
#df = spark.read.option("recursiveFileLookup", "true").parquet("wat_parquet/")
df = spark_session.read.parquet("wet_parquet/wet_parquet_files_0_1.parquet")

df.show(10)
