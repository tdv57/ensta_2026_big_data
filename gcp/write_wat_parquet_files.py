from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import urlparse
from datetime import timedelta
from LOG_MESSAGE import INFO, WARNING
import download_wat as dwat
import download_wet_paths as dwet_paths
import json
import time
import sys
import os

GCS_BUCKET = os.environ.get("GCS_BUCKET", "gs://ensta-bigdata-2024")


def extract_host_and_path(url):
    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path + ("?" + parsed.query if parsed.query else "")
    return host, path


def is_response(metadata_json):
    try:
        return "HTTP-Response-Metadata" in metadata_json["Envelope"]["Payload-Metadata"]
    except KeyError:
        return False


def get_title(metadata_json):
    try:
        return metadata_json["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Head"]["Title"]
    except KeyError:
        return "None"


def get_uri_host_path(metadata_json):
    try:
        uri = metadata_json["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
        host, path = extract_host_and_path(uri)
        return uri, host, path
    except KeyError:
        return "None", "None", "None"


def format_duration(seconds):
    return str(timedelta(seconds=int(seconds)))


def progress_bar(current, total, start_time, bar_width=40):
    pct = current / total if total > 0 else 0
    filled = int(bar_width * pct)
    bar = "=" * filled + "-" * (bar_width - filled)
    elapsed = time.time() - start_time
    if current > 0:
        eta_str = format_duration(elapsed / current * (total - current))
    else:
        eta_str = "??:??:??"
    print(
        f"\r  [{bar}] {current}/{total} ({100*pct:.1f}%)"
        f" | Ecoule: {format_duration(elapsed)} | Restant: ~{eta_str}   ",
        end="", flush=True
    )


def wat_urls_to_parquet(spark_session, schema, wet_urls, parquet_name, pas):
    BATCH_SIZE = 100000
    rows = []
    n_total = 0

    urls_to_process = [(n, u) for n, u in enumerate(wet_urls) if n % pas == 0]
    n_urls = len(urls_to_process)
    print(INFO + f"URLs effectives a traiter : {n_urls}")
    job_start = time.time()

    for url_index, (n_url, wet_url) in enumerate(urls_to_process):
        progress_bar(url_index, n_urls, job_start)

        wat_url = dwat.wet_to_wat_url(wet_url)
        wat_response = dwat.get_wat_response(wat_url)
        if wat_response is None:
            continue

        wat_stream = ArchiveIterator(wat_response.raw)

        for record in wat_stream:
            warc_id  = record.rec_headers.get_header("WARC-Record-ID")
            warc_ref = record.rec_headers.get_header("WARC-Refers-To")

            raw = record.content_stream().read().decode("utf-8", errors="ignore")
            try:
                metadata_json = json.loads(raw)
            except (json.JSONDecodeError, AttributeError):
                continue

            if not is_response(metadata_json):
                continue

            n_total += 1
            uri, host, path = get_uri_host_path(metadata_json)
            title = get_title(metadata_json)
            rows.append((warc_id, warc_ref, title, uri, host, path))

            if len(rows) >= BATCH_SIZE:
                print()
                print(INFO + f"Ecriture batch de {len(rows):,} lignes dans GCS")
                df = spark_session.createDataFrame(
                    spark_session.sparkContext.parallelize(rows), schema=schema
                )
                df.write.mode("append").parquet(
                    f"{GCS_BUCKET}/wat_parquet/{parquet_name}.parquet"
                )
                rows = []

    progress_bar(n_urls, n_urls, job_start)
    print()

    if len(rows) > 0:
        print(INFO + f"Ecriture batch final de {len(rows):,} lignes dans GCS")
        df = spark_session.createDataFrame(
            spark_session.sparkContext.parallelize(rows), schema=schema
        )
        df.write.mode("append").parquet(
            f"{GCS_BUCKET}/wat_parquet/{parquet_name}.parquet"
        )

    total_time = time.time() - job_start
    print(INFO + f"-----------------------------------------")
    print(INFO + f"Duree totale WAT           : {format_duration(total_time)}")
    print(INFO + f"Enregistrements WAT ecrits : {n_total:,}")
    print(INFO + f"-----------------------------------------")
    return n_total


def write_wat_parquet_files(spark_session, first_url, last_url, pas):
    schema = StructType([
        StructField("WARC_ID",        StringType(), True),
        StructField("WARC_REFERS_TO", StringType(), True),
        StructField("TITRE",          StringType(), True),
        StructField("URI",            StringType(), True),
        StructField("HOST",           StringType(), True),
        StructField("PATH",           StringType(), True),
    ])

    parquet_name = f"wat_parquet_files_{first_url}_{last_url}"
    all_wet_urls = dwet_paths.get_all_wet_urls(min_year=2024, max_year=2025)
    print(INFO + f"Total URLs disponibles : {len(all_wet_urls):,}")

    wet_urls = all_wet_urls[first_url:last_url]
    print(INFO + f"URLs selectionnees : {len(wet_urls):,} (index {first_url} a {last_url}, pas={pas})")

    n_total = wat_urls_to_parquet(
        spark_session=spark_session,
        schema=schema,
        wet_urls=wet_urls,
        parquet_name=parquet_name,
        pas=pas,
    )
    print(INFO + f"Resultat ecrit dans {GCS_BUCKET}/wat_parquet/{parquet_name}.parquet")
    return n_total


def main():
    if len(sys.argv) < 4:
        print("Usage: write_wat_parquet_files.py <first_url> <last_url> <pas>")
        sys.exit(1)
    first_url = int(sys.argv[1])
    last_url  = int(sys.argv[2])
    pas       = int(sys.argv[3])
    spark_session = SparkSession.builder.getOrCreate()
    write_wat_parquet_files(spark_session, first_url, last_url, pas)
    spark_session.stop()


if __name__ == "__main__":
    main()
