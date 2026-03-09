from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from LOG_MESSAGE import INFO, DEBUG, ERROR, WARNING
import download_warc as dwarc
import download_wat as dwat 
import download_wet as dwet 
from warcio.archiveiterator import ArchiveIterator
from datetime import datetime
import time
import json 
import sys 
#Envelope Payload-Metadata WARC-Metadata-Metadata (ensuite c'est une liste jusquà Name:"languages-cld2") 
def get_languages_from_wat_page(metadata_json): 
    warc_languages = []
    try : 
        warc_metadata = (
        metadata_json.get("Envelope", {})
                    .get("Payload-Metadata", {})
                    .get("WARC-Metadata-Metadata", [])
                    .get("Metadata-Records", [])
        )
        if warc_metadata != [] : 
            for item in warc_metadata:
                if item.get("Name", {}) == "languages-cld2":
                    cld2 = item.get("Value", {})
                    cld2_json = json.loads(cld2)
                    languages = cld2_json.get("languages", [])
                    for language in languages : 
                        warc_languages.append(language.get("name", {}))
                    return warc_languages
        return ["None"]
    except (json.JSONDecodeError, AttributeError) : 
        return ["None"]

def wat_urls_to_parquet(spark_session, schema, wat_urls, parquet_name) : 
    print(INFO + "writing parquet file for wat files")
    BATCH_SIZE = 1000000
    rows = []
    for (n_url,wat_url) in enumerate(wat_urls): 
        print(INFO + f"Traitement de l'url {n_url}") 
        wat_response = dwat.get_wat_response(wat_url)
        wat_stream = ArchiveIterator(wat_response.raw)
        
        start = time.time()
        for record in wat_stream : 

            
            
            warc_id = record.rec_headers.get_header("WARC-Record-ID")
            metadata = record.content_stream().read().decode("utf-8", errors="ignore")
            try: 
                metadata_json = json.loads(metadata)
            except (json.JSONDecodeError, AttributeError) : 
                continue
            languages = get_languages_from_wat_page(metadata_json)

            rows.append((warc_id, languages))

            if len(rows) >= BATCH_SIZE:
                print(INFO + f"writing parquet file with {len(rows)} rows for wat files")
                start = time.time()
                df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(rows), schema=schema)
                df.write.mode("append").parquet(f"wat_parquet/{parquet_name}.parquet")
                end = time.time()
                print(f"Temps d'écriture {end-start}")
                rows = []
        end = time.time()
        print(f"Différence de temps pour languages= {end-start}")

    if len(rows) >= 0:
        df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(rows), schema=schema)
        df.write.mode("append").parquet(f"wat_parquet/{parquet_name}.parquet")

def main(): 
    spark_session = SparkSession.builder.getOrCreate()

    schema_struct_type =  \
    [ 
        StructField("WARC_ID", StringType(), True),
        StructField("LANG", ArrayType(StringType()), True)
    ]

    schema = StructType(schema_struct_type)

    parquet_name = f"wat_parquet_files_{int(sys.argv[1])}_{int(sys.argv[2])}"
    wat_urls = dwat.get_wat_urls()[int(sys.argv[1]):int(sys.argv[2])] # 2700000 urls pour aller de 2026 à 2023
    print(f"Il y a au total {len(dwat.get_wat_urls())}")
    wat_urls_to_parquet(spark_session, schema, wat_urls, parquet_name)

    print(INFO + "Creating parquet files from wat files")

if __name__ == "__main__" : 
    main()