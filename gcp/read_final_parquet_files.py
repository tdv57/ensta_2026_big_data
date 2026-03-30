from pyspark.sql import SparkSession
from LOG_MESSAGE import INFO
import os

GCS_BUCKET = os.environ.get("GCS_BUCKET", "gs://ensta-bigdata-2024")
FINAL_PARQUET = f"{GCS_BUCKET}/final_parquet"

MONTHS = [
    "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
    "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"
]
TLDS = [".ru", ".com", ".fr", ".uk", ".de", ".jp", ".cn", ".us", ".br", ".in"]


def get_total_occurences(df_final):
    trump  = df_final.filter(df_final.Target0 > 0).count()
    harris = df_final.filter(df_final.Target1 > 0).count()
    biden  = df_final.filter(df_final.Target2 > 0).count()
    print(INFO + "=== Total occurrences ===")
    print(f"  Trump  : {trump}")
    print(f"  Harris : {harris}")
    print(f"  Biden  : {biden}")
    return {"Trump": trump, "Harris": harris, "Biden": biden}


def get_occurence_by_months(df_final):
    print(INFO + "=== Occurrences par mois ===")
    occurences = {}
    for i in range(1, 13):
        df_month = df_final.filter(df_final.MONTH == i)
        trump  = df_month.filter(df_final.Target0 > 0).count()
        harris = df_month.filter(df_final.Target1 > 0).count()
        biden  = df_month.filter(df_final.Target2 > 0).count()
        month_name = MONTHS[i - 1]
        print(f"  {month_name:12} → Trump={trump:6}  Harris={harris:6}  Biden={biden:6}")
        occurences[month_name] = {"Trump": trump, "Harris": harris, "Biden": biden}
    return occurences


def get_occurence_by_TLD(df_final):
    print(INFO + "=== Occurrences par TLD ===")
    occurences = {}
    for tld in TLDS:
        df_tld = df_final.filter(df_final.HOST.endswith(tld))
        trump  = df_tld.filter(df_final.Target0 > 0).count()
        harris = df_tld.filter(df_final.Target1 > 0).count()
        biden  = df_tld.filter(df_final.Target2 > 0).count()
        print(f"  {tld:6} → Trump={trump:6}  Harris={harris:6}  Biden={biden:6}")
        occurences[tld] = {"Trump": trump, "Harris": harris, "Biden": biden}
    return occurences


def main():
    spark_session = SparkSession.builder.getOrCreate()

    print(INFO + f"Lecture du parquet final : {FINAL_PARQUET}")
    df_final = (
        spark_session.read
        .option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .parquet(FINAL_PARQUET)
    )

    print(INFO + f"Nombre total de lignes : {df_final.count()}")
    df_final.show(10)

    get_total_occurences(df_final)
    get_occurence_by_months(df_final)
    get_occurence_by_TLD(df_final)

    spark_session.stop()


if __name__ == "__main__":
    main()
