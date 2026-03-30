"""
read_results.py
---------------
Lit les fichiers Parquet finaux (local ou GCS) et affiche les résultats.

Usage :
    python read_results.py                              # lit ./final_parquet/ par défaut
    python read_results.py <path>                       # chemin local
    python read_results.py gs://ensta-bigdata-2024/final_parquet/  # GCS
"""

from pyspark.sql import SparkSession
from LOG_MESSAGE import INFO
import sys
import os

MONTHS = [
    "Janvier", "Fevrier", "Mars", "Avril", "Mai", "Juin",
    "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"
]
TLDS = [".com", ".fr", ".de", ".uk", ".ru", ".br", ".us", ".jp", ".cn", ".in"]


def get_total_occurences(df_final):
    trump  = df_final.filter(df_final.Target0 > 0).count()
    harris = df_final.filter(df_final.Target1 > 0).count()
    biden  = df_final.filter(df_final.Target2 > 0).count()
    total  = df_final.count()
    print(INFO + "=== Total occurrences ===")
    print(f"  Pages analysees : {total:,}")
    print(f"  Trump           : {trump:,}  ({100*trump/max(total,1):.1f}%)")
    print(f"  Harris          : {harris:,}  ({100*harris/max(total,1):.1f}%)")
    print(f"  Biden           : {biden:,}  ({100*biden/max(total,1):.1f}%)")
    return {"Trump": trump, "Harris": harris, "Biden": biden}


def get_occurence_by_months(df_final):
    print(INFO + "=== Occurrences par mois ===")
    for i in range(1, 13):
        df_month = df_final.filter(df_final.MONTH == i)
        count = df_month.count()
        if count == 0:
            continue
        trump  = df_month.filter(df_month.Target0 > 0).count()
        harris = df_month.filter(df_month.Target1 > 0).count()
        biden  = df_month.filter(df_month.Target2 > 0).count()
        print(f"  {MONTHS[i-1]:12} -> Trump={trump:6,}  Harris={harris:6,}  Biden={biden:6,}")


def get_occurence_by_TLD(df_final):
    print(INFO + "=== Occurrences par TLD ===")
    for tld in TLDS:
        df_tld = df_final.filter(df_final.HOST.endswith(tld))
        count = df_tld.count()
        if count == 0:
            continue
        trump  = df_tld.filter(df_tld.Target0 > 0).count()
        harris = df_tld.filter(df_tld.Target1 > 0).count()
        biden  = df_tld.filter(df_tld.Target2 > 0).count()
        print(f"  {tld:6} -> Trump={trump:6,}  Harris={harris:6,}  Biden={biden:6,}")


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "final_parquet"

    # Vérifier que le chemin existe si c'est un chemin local
    if not path.startswith("gs://") and not os.path.exists(path):
        print(f"Erreur : le chemin '{path}' n'existe pas.")
        sys.exit(1)

    spark_session = SparkSession.builder.getOrCreate()

    print(INFO + f"Lecture de : {path}")
    df_final = (
        spark_session.read
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .parquet(path)
    )

    total = df_final.count()
    print(INFO + f"Nombre total de lignes : {total:,}")
    df_final.show(10)

    get_total_occurences(df_final)
    get_occurence_by_months(df_final)
    get_occurence_by_TLD(df_final)

    spark_session.stop()


if __name__ == "__main__":
    main()
