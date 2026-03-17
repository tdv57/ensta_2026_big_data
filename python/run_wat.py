import download_wat_paths as dwat_path
from CC_name import get_CC_names
import download_wat as dwat 
import write_wat_parquet_files as watp
from pyspark.sql import SparkSession
import sys 
from LOG_MESSAGE import ERROR
import os

def run_wat(first_url, last_url, pas, port):
    CC_archive_names = get_CC_names(min_year=2024, max_year=2025)
    if not os.path.exists("wat_paths_gz"):
        for CC_archive_name in CC_archive_names:
            dwat_path.download_wat_paths(CC_archive_name)
    spark_session = SparkSession.builder.config("spark.ui.port", port).getOrCreate()
    downloaded_name = f"{first_url}_{last_url}"
    watp.write_wat_parquet_files(
        spark_session=spark_session,
        downloaded_name=downloaded_name,
        first_url = first_url,
        last_url = last_url, 
        pas = pas 
        )
    
if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(ERROR + "run_wet doit recevoir 4 arguments" )
        print("first_url qui représente le numéro de la première url à télécharger dans l'ensemble des urls disponibles")
        print("last_url qui représente le numéro de la dernière url à télécharger dans l'ensemble des urls disponibles")
        print("pas qui représente le nombre d'url qui sont skip entre deux téléchargement d'url")
        print("port qui est le port sur lequel tournera spark")
    first_url = int(sys.argv[1])
    last_url = int(sys.argv[2])
    pas = int(sys.argv[3])
    port = int(sys.argv[4])
    run_wat(first_url, last_url, pas, port)
    