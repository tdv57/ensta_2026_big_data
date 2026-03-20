from pyspark.sql import SparkSession
from LOG_MESSAGE import INFO
import sys
import os

GCS_BUCKET = os.environ.get("GCS_BUCKET", "gs://ensta-bigdata-2024")


def write_final_parquet(spark_session, first_url, last_url, pas):
    """
    Joint les parquets WAT et WET sur WARC_REFERS_TO
    et écrit le résultat final dans GCS.

    WAT contient : WARC_REFERS_TO, TITRE, URI, HOST, PATH
    WET contient : WARC_REFERS_TO, YEAR, MONTH, DAY, Target0, Target1, Target2

    La jointure sur WARC_REFERS_TO permet d'associer
    les métadonnées de la page (WAT) aux occurrences de mots-clés (WET).
    """
    url_key = f"{first_url}_{last_url}"
    wet_path = f"{GCS_BUCKET}/wet_parquet/wet_parquet_files_{url_key}.parquet"
    wat_path = f"{GCS_BUCKET}/wat_parquet/wat_parquet_files_{url_key}.parquet"
    out_path = f"{GCS_BUCKET}/final_parquet/final_parquet_files_{url_key}.parquet"

    print(INFO + f"Lecture WET : {wet_path}")
    df_wet = spark_session.read.parquet(wet_path)

    print(INFO + f"Lecture WAT : {wat_path}")
    df_wat = spark_session.read.parquet(wat_path)

    # Suppression de WARC_ID avant jointure pour éviter les colonnes dupliquées
    # (on garde uniquement WARC_REFERS_TO comme clé de jointure)
    df_wet = df_wet.drop("WARC_ID")
    df_wat = df_wat.drop("WARC_ID")

    df_joined = df_wet.join(df_wat, on="WARC_REFERS_TO", how="inner")

    print(INFO + f"Écriture final parquet : {out_path}")
    df_joined.write.mode("overwrite").parquet(out_path)
    print(INFO + f"Jointure terminée : {df_joined.count()} lignes")


def main():
    if len(sys.argv) < 3:
        print("Usage: write_final_parquet.py <first_url> <last_url>")
        sys.exit(1)

    first_url = int(sys.argv[1])
    last_url  = int(sys.argv[2])
    pas       = int(sys.argv[3]) if len(sys.argv) > 3 else 100000

    spark_session = SparkSession.builder.getOrCreate()
    write_final_parquet(spark_session, first_url, last_url, pas)
    spark_session.stop()


if __name__ == "__main__":
    main()
