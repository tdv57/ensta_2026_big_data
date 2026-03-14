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
import os
from urllib.parse import urlparse

def extract_host_and_path(url):

    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path + ('?' + parsed.query if parsed.query else '')
    
    return host, path

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

def is_response(metadata_json):
    try : 
        payload_metadata = metadata_json["Envelope"]["Payload-Metadata"]
    except KeyError as e:
        return False 
    if "HTTP-Response-Metadata" in payload_metadata:
        return True 
    else : 
        return False 

def get_title(metadata_json):
    try:
        return metadata_json["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Head"]["Title"]
    except KeyError: 
        return "None"

def get_warc_record_id(metadata_json):
    try:
        return metadata_json["Envelope"]["WARC-Header-Metadata"]["WARC-Record-ID"]
    except KeyError:
        return "None"
    
def get_uri_host_path(metadata_json):
    try : 
        uri = metadata_json["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
        host, path = extract_host_and_path(uri)
        return uri, host, path
    except KeyError:
        return "None", "None", "None"
    

def wat_urls_to_parquet(spark_session, schema, wat_urls, parquet_name) : 
    print(INFO + "writing parquet file for wat files")
    BATCH_SIZE = 100000
    rows = []
    for (n_url,wat_url) in enumerate(wat_urls): 
        start = time.time()
        print(INFO + f"Traitement de l'url {n_url}") 
        wat_response = dwat.get_wat_response(wat_url)
        if wat_response is None: 
            continue
        wat_stream = ArchiveIterator(wat_response.raw)
        
        for record in wat_stream : 

            
            
            warc_id = record.rec_headers.get_header("WARC-Record-ID")
            metadata = record.content_stream().read().decode("utf-8", errors="ignore")
            try: 
                metadata_json = json.loads(metadata)
            except (json.JSONDecodeError, AttributeError) : 
                continue
            if not (is_response(metadata_json)):
                #print(DEBUG + "not a response")
                continue
            else : 
                pass
                #print(DEBUG + "is a response")
            #languages = get_languages_from_wat_page(metadata_json)
            uri, host, path = get_uri_host_path(metadata_json)
            title = get_title(metadata_json)
            rows.append((warc_id, title, uri, host, path))

            if len(rows) >= BATCH_SIZE:
                print(INFO + f"writing parquet file with {len(rows)} rows for wat files")
                start_writing = time.time()
                df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(rows), schema=schema)
                df.write.mode("append").parquet(f"wat_parquet/{parquet_name}.parquet")
                end_writing = time.time()
                print(f"Temps d'écriture {end_writing-start_writing}")
                rows = []
        end = time.time()
        print(INFO + f"url traité en {end-start} secondes")

    if len(rows) >= 0:
        df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(rows), schema=schema)
        df.write.mode("append").parquet(f"wat_parquet/{parquet_name}.parquet")

def main(): 
    spark_session = SparkSession.builder.getOrCreate()

    

    if not os.path.exists("wat_urls_downloaded"):
        with open("wat_urls_downloaded", "w") as f:
            pass  

    downloaded_name = f"{sys.argv[1]}_{sys.argv[2]}"
    with open("wat_urls_downloaded","r") as f :
        for line in f:
            if line.strip() == downloaded_name:
                print(INFO + "url déjà téléchargé")
                return 

    schema_struct_type =  \
    [ 
        StructField("WARC_ID", StringType(), True),
        #StructField("LANG", ArrayType(StringType()), True),
        StructField("TITRE", StringType(), True),
        StructField("URI", StringType(), True),
        StructField("HOST", StringType(), True),
        StructField("PATH", StringType(), True),
    ]

    schema = StructType(schema_struct_type)

    parquet_name = f"wat_parquet_files_{int(sys.argv[1])}_{int(sys.argv[2])}"
    wat_urls = dwat.get_wat_urls()[int(sys.argv[1]):int(sys.argv[2])] # 2700000 urls pour aller de 2026 à 2023
    print(f"Il y a au total {len(dwat.get_wat_urls())}")
    wat_urls_to_parquet(spark_session, schema, wat_urls, parquet_name)

    print(INFO + "Creating parquet files from wat files")
    with open("wat_urls_downloaded", "a") as f:
        f.write(downloaded_name + "\n")

if __name__ == "__main__" : 
    main()