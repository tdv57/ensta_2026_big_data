from pyspark.sql import SparkSession

spark_session = SparkSession.builder.getOrCreate()

df = spark_session.read.parquet("wet_output_spark.parquet")

df.show(10)

df.stop()