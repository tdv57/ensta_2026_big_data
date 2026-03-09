from pyspark.sql import SparkSession

spark_session = SparkSession.builder.getOrCreate()

df = spark_session.read.parquet("wat_output_spark.parquet")

df.show(100)
