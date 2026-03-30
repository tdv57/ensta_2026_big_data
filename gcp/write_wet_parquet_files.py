from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from warcio.archiveiterator import ArchiveIterator
from datetime import datetime, timedelta
from LOG_MESSAGE import INFO, WARNING
import download_wet as dwet
import download_wet_paths as dwet_paths
import time
import sys
import os

GCS_BUCKET = os.environ.get("GCS_BUCKET", "gs://ensta-bigdata-2024")

TARGETS = [["trump"], ["harris"], ["biden"]]


def count_occurence(targets, text):
    text_lower = text.lower()
    return [
        sum(text_lower.count(word.lower()) for word in target)
        for target in targets
    ]


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


def wet_urls_to_parquet(spark_session, schema, wet_urls, parquet_name, pas):
    BATCH_SIZE = 100000
    rows = []
    n_total = 0
    n_matched = 0
    zero_counts = [0] * len(TARGETS)

    urls_to_process = [(n, u) for n, u in enumerate(wet_urls) if n % pas == 0]
    n_urls = len(urls_to_process)
    print(INFO + f"URLs effectives a traiter : {n_urls}")
    job_start = time.time()

    for url_index, (n_url, wet_url) in enumerate(urls_to_process):
        progress_bar(url_index, n_urls, job_start)

        wet_response = dwet.get_wet_response(wet_url)
        if wet_response is None:
            continue

        wet_stream = ArchiveIterator(wet_response.raw)

        for record in wet_stream:
            n_total += 1
            warc_id  = record.rec_headers.get_header("WARC-Record-ID")
            warc_ref = record.rec_headers.get_header("WARC-Refers-To")
            date_str = record.rec_headers.get_header("WARC-Date")
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", ""))
                year, month, day = dt.year, dt.month, dt.day
            except Exception:
                year, month, day = None, None, None

            text = record.content_stream().read().decode("utf-8", errors="ignore")
            counts = count_occurence(TARGETS, text)
            if counts != zero_counts:
                n_matched += 1
                rows.append((warc_id, warc_ref, year, month, day, *counts))

            if len(rows) >= BATCH_SIZE:
                print()
                print(INFO + f"Ecriture batch de {len(rows):,} lignes dans GCS")
                df = spark_session.createDataFrame(
                    spark_session.sparkContext.parallelize(rows), schema=schema
                )
                df.write.mode("append").parquet(
                    f"{GCS_BUCKET}/wet_parquet/{parquet_name}.parquet"
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
            f"{GCS_BUCKET}/wet_parquet/{parquet_name}.parquet"
        )

    total_time = time.time() - job_start
    print(INFO + f"-----------------------------------------")
    print(INFO + f"Duree totale WET      : {format_duration(total_time)}")
    print(INFO + f"Pages lues            : {n_total:,}")
    print(INFO + f"Pages avec occurrence : {n_matched:,} ({100*n_matched/max(n_total,1):.2f}%)")
    print(INFO + f"-----------------------------------------")
    return n_total, n_matched


def write_wet_parquet_files(spark_session, first_url, last_url, pas):
    schema = StructType(
        [
            StructField("WARC_ID",        StringType(),  True),
            StructField("WARC_REFERS_TO", StringType(),  True),
            StructField("YEAR",           IntegerType(), True),
            StructField("MONTH",          IntegerType(), True),
            StructField("DAY",            IntegerType(), True),
        ]
        + [StructField(f"Target{i}", IntegerType(), True) for i in range(len(TARGETS))]
    )

    parquet_name = f"wet_parquet_files_{first_url}_{last_url}"
    all_wet_urls = dwet_paths.get_all_wet_urls(min_year=2024, max_year=2025)
    print(INFO + f"Total URLs disponibles : {len(all_wet_urls):,}")

    wet_urls = all_wet_urls[first_url:last_url]
    print(INFO + f"URLs selectionnees : {len(wet_urls):,} (index {first_url} a {last_url}, pas={pas})")
    print(INFO + f"Targets : {TARGETS}")

    n_total, n_matched = wet_urls_to_parquet(
        spark_session=spark_session,
        schema=schema,
        wet_urls=wet_urls,
        parquet_name=parquet_name,
        pas=pas,
    )
    print(INFO + f"Resultat ecrit dans {GCS_BUCKET}/wet_parquet/{parquet_name}.parquet")
    return n_total, n_matched


def main():
    if len(sys.argv) < 4:
        print("Usage: write_wet_parquet_files.py <first_url> <last_url> <pas>")
        sys.exit(1)
    first_url = int(sys.argv[1])
    last_url  = int(sys.argv[2])
    pas       = int(sys.argv[3])
    spark_session = SparkSession.builder.getOrCreate()
    write_wet_parquet_files(spark_session, first_url, last_url, pas)
    spark_session.stop()


if __name__ == "__main__":
    main()
