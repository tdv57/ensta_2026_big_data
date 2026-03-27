from write_wet_parquet_files import gcp_write_wet_parquet_files
from pyspark.sql import SparkSession
import sys 
from LOG_MESSAGE import ERROR 
def main():
    bucket_path = sys.argv[1]
    if bucket_path[:5] != "gs://":
        raise ValueError(ERROR + "bucket cloud devrait commencer par gs://")
    first_url = int(sys.argv[2])
    last_url = int(sys.argv[3])
    pas = int(sys.argv[4])

    spark_session = SparkSession.builder.getOrCreate()
    gcp_write_wet_parquet_files(spark_session=spark_session,
                                downloaded_name="gcp_wet_parquet_0_10",
                                bucket_path=bucket_path,
                                first_url=first_url,
                                last_url=last_url,
                                pas=pas)
    
if __name__ == "__main__":
    main()