from write_wat_parquet_files import write_wat_parquet_files
from pyspark.sql import SparkSession
from LOG_MESSAGE import INFO, ERROR
import sys


def run_wat(first_url, last_url, pas):
    """
    Lance le traitement WAT :
    1. Récupère la liste des URLs WET en mémoire
    2. Convertit chaque URL WET en URL WAT
    3. Lit les métadonnées WAT et écrit les parquets dans GCS
    """
    spark_session = SparkSession.builder.getOrCreate()
    print(INFO + f"Lancement WAT : URLs {first_url} à {last_url}, pas={pas}")
    n_total = write_wat_parquet_files(spark_session, first_url, last_url, pas)
    print(INFO + f"WAT terminé : {n_total} enregistrements écrits")
    spark_session.stop()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(ERROR + "Usage: run_wat.py <first_url> <last_url> <pas>")
        print("  first_url : index de la première URL à traiter")
        print("  last_url  : index de la dernière URL à traiter")
        print("  pas       : 1 URL traitée toutes les <pas> URLs")
        sys.exit(1)

    run_wat(
        first_url=int(sys.argv[1]),
        last_url=int(sys.argv[2]),
        pas=int(sys.argv[3]),
    )
