from write_wet_parquet_files import write_wet_parquet_files
from pyspark.sql import SparkSession
from LOG_MESSAGE import INFO, ERROR
import sys


def run_wet(first_url, last_url, pas):
    """
    Lance le traitement WET :
    1. Récupère la liste des URLs WET en mémoire (pas de fichier local)
    2. Traite les URLs et écrit les parquets dans GCS
    """
    spark_session = SparkSession.builder.getOrCreate()
    print(INFO + f"Lancement WET : URLs {first_url} à {last_url}, pas={pas}")
    n_total, n_matched = write_wet_parquet_files(spark_session, first_url, last_url, pas)
    print(INFO + f"WET terminé : {n_matched}/{n_total} pages avec occurrences")
    spark_session.stop()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(ERROR + "Usage: run_wet.py <first_url> <last_url> <pas>")
        print("  first_url : index de la première URL à traiter")
        print("  last_url  : index de la dernière URL à traiter")
        print("  pas       : 1 URL traitée toutes les <pas> URLs")
        sys.exit(1)

    run_wet(
        first_url=int(sys.argv[1]),
        last_url=int(sys.argv[2]),
        pas=int(sys.argv[3]),
    )
