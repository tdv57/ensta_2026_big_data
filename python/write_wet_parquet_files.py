from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from LOG_MESSAGE import INFO, DEBUG, ERROR, WARNING
import download_warc as dwarc
import download_wat as dwat 
import download_wet as dwet 
from warcio.archiveiterator import ArchiveIterator
from datetime import datetime
import time
import sys 
import os 
# Créer un parquet file à partir des wet files 

# Faire un champ pour compter les occurences de chaque variable 

# Faire un champ texte brut
TARGETS = [["trump"], ["harris"],["biden"]]
# Entrée: tableau des targets exemple = [["trump"],["harris", "kamala harris"]] et le texte brut
# Sortie : tableau du nombre d'occurence pour chaque target [2, 4] par exemple
def count_occurence(targets, text) : 
    text_lower = text.lower()
    occurence = []
    for target in targets :
        s = 0 
        for target_word in target : 
            s += text_lower.count(target_word.lower())
        occurence.append(s)
    return occurence

# Il faut certainement enlever wet_response et travailler avec les urls 
# Le soucis actuel c'est qu'on remplit jamais la limite de batch et donc on va écrire je dirai 
# 500 lignes par 500 lignes, si on peut pousser le truc à écrire 10000 lignes par lignes on pourrait gagner bcp 

def wet_urls_to_parquet(spark_session, schema, wet_urls, targets, parquet_name, pas) : 
    BATCH_SIZE = 1000000

    rows = []
    n_no_occurence_found = 0 
    n_file = 0
    print(INFO + "writing parquet file for wet files")
    for (n_url, wet_url) in enumerate(wet_urls):
        if n_url % pas != 0:
            continue
        start = time.time()
        print(INFO + f"Traitement de l'url {n_url}") 
        wet_response = dwet.get_wet_response(wet_url)
        if wet_response is None :
            continue 
        wet_stream = ArchiveIterator(wet_response.raw)
        

        for record in wet_stream :
            n_file += 1

            warc_id = record.rec_headers.get_header("WARC-Record-ID")
            warc_refers_to = record.rec_headers.get_header("WARC-Refers-To")
            date_str = record.rec_headers.get_header("WARC-Date")
            dt = datetime.fromisoformat(date_str.replace("Z", ""))
            year = dt.year
            month = dt.month
            day = dt.day
           
            text = record.content_stream().read().decode("utf-8", errors="ignore")
            counts = count_occurence(targets, text)
            if (counts != ([0]*len(targets))):
                #print(INFO + "fichier comportant la target trouvé")
                rows.append((warc_id, warc_refers_to, year, month, day,  *counts))
            else : 
                n_no_occurence_found +=1
            if len(rows) >= BATCH_SIZE:
                print(INFO + f"writing parquet file with {len(rows)} rows for wet files")
                start_writing = time.time()
                df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(rows), schema=schema)
                df.write.mode("append").parquet(f"wet_parquet/{parquet_name}.parquet")
                end_writing = time.time()
                print(f"Temps d'écriture {end_writing-start_writing}")
                rows = []
        
        end = time.time()
        print(INFO + f"url traité en {end-start} secondes")
    if len(rows) >= 0:
        df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(rows), schema=schema)
        df.write.mode("append").parquet(f"wet_parquet/{parquet_name}.parquet")
    print(INFO + f"{n_file} fichiers analysés et dont {n_no_occurence_found} fichiers ne comporte pas d'occurence")
    return n_file, n_no_occurence_found

def gcp_wet_urls_to_parquet(iterator):
    # On m'a dit de remettre les imports car dataproc n'a pas toutes les librairies qu'il faudra alors retélécharger

    import requests
    from warcio.archiveiterator import ArchiveIterator
    from datetime import datetime

    session = requests.Session()

    targets = TARGETS
    n_file = 0
    n_no_occurrence = 0 
    for row in iterator:
        url = row.url 
        url = "https://data.commoncrawl.org/" + url
        try:
            print(DEBUG + f"{url}")
            response = session.get(url, stream=True, timeout=10)
            if response.status_code != 200:
                continue
                
            stream = ArchiveIterator(response.raw)

            for record in stream:
                text = record.content_stream().read().decode("utf-8", errors="ignore")
                n_file += 1 
                counts = count_occurence(targets, text)
                if counts != [0]*len(targets):
                    print(DEBUG + "occurence")
                    date_str = record.rec_headers.get_header("WARC-Date")
                    dt = datetime.fromisoformat(date_str.replace("Z", ""))

                    yield (
                        record.rec_headers.get_header("WARC-Record-ID"),
                        record.rec_headers.get_header("WARC-Refers-To"),
                        dt.year, dt.month, dt.day,
                        *counts
                    ) # En gros yield va envoyer ligne par ligne le résultat à spark
                    # car on va utiliser cette fonction dans le paradigme des fonctions d'ordre sup
                    # on fera un truc du genre df_urls.transfo_rdd.mapPArtion(gcp_wet...)
                else :
                    print(DEBUG + "no_occurence")
                    n_no_occurrence += 1 
        except :
            print(ERROR + f"{url}")
            continue

    yield ("__STATS__", n_file, n_no_occurrence)

def gcp_write_wet_parquet_files(spark_session,downloaded_name, bucket_path, first_url, last_url, pas):
    print(INFO + f"bucketpath = {bucket_path} for write_wet")
    schema_struct_type =  \
    [ 
        StructField("WARC_ID", StringType(), True),
        StructField("WARC_REFERS_TO", StringType(), True),
        StructField("YEAR", IntegerType(), True),
        StructField("MONTH", IntegerType(), True),
        StructField("DAY", IntegerType(), True)
    ] \
    + \
    [StructField(f"Target{n_target}", IntegerType(), True) for n_target in range(len(TARGETS))]

    schema = StructType(schema_struct_type)
    parquet_name = f"wet_parquet_files_{first_url}_{last_url}"
    if not bucket_path.startswith("gs://"):
        raise ValueError("bucket_path should start by gs://")
    df_urls = dwet.gcp_build_df_urls(spark_session=spark_session,
                                     bucket_path=bucket_path,
                                     first_url=first_url,
                                     last_url=last_url,
                                     pas=pas
                                     )
    print("[DEBUG] Nombre d’éléments dans le df_urls:", df_urls.count())
    result_rdd = df_urls.rdd.mapPartitions(gcp_wet_urls_to_parquet)
    print("[DEBUG] Nombre d’éléments dans le RDD:", result_rdd.count())
    stats_rdd = result_rdd.filter(lambda x: x[0] == "__STATS__")
    print("[DEBUG] Nombre d’éléments dans le stats_rdd:", stats_rdd.count())
    total_file = stats_rdd.map(lambda x: x[1]).sum()
    print("[DEBUG] Nombre d’éléments dans le total_file:", total_file)
    total_no_occurrence = stats_rdd.map(lambda x: x[2]).sum()
    print("[DEBUG] Nombre d’éléments dans le total_no_occurence:", total_no_occurrence)

    # Les lignes valides pour le DataFrame
    data_rdd = result_rdd.filter(lambda x: x[0] != "__STATS__")
    print("[DEBUG] Nombre d’éléments dans le data_rdd", data_rdd.count())
    result_df = spark_session.createDataFrame(data_rdd, schema)

    result_df.write.mode("append").parquet(
        f"{bucket_path}/wet_parquet/{parquet_name}"
    )
    from google.cloud import storage

    client = storage.Client()
    bucket_name = bucket_path[5:]
    bucket = client.bucket(bucket_name)
    blob = bucket.blob("wet_parquet_extra_info")
    extra_info = f"{downloaded_name};{total_file};{total_no_occurrence}"
    if blob.exists():
        old_content = blob.download_as_text()
        extra_info = old_content + "\n" + extra_info
    blob.upload_from_string(extra_info)


def write_wet_parquet_files(spark_session, downloaded_name, first_url, last_url, pas):

    schema_struct_type =  \
    [ 
        StructField("WARC_ID", StringType(), True),
        StructField("WARC_REFERS_TO", StringType(), True),
        StructField("YEAR", IntegerType(), True),
        StructField("MONTH", IntegerType(), True),
        StructField("DAY", IntegerType(), True)
    ] \
    + \
    [StructField(f"Target{n_target}", IntegerType(), True) for n_target in range(len(TARGETS))]

    schema = StructType(schema_struct_type)

    parquet_name = f"wet_parquet_files_{first_url}_{last_url}"
    wet_urls = dwet.get_wet_urls()[first_url:last_url]
    print(INFO + f"Il y a au total {len(dwet.get_wet_urls())}") #2700000 urls entre 2026 et 2023
    print(INFO + f"Creating parquet files for target {TARGETS}")
    n_total_file, n_total_no_occurence_found = wet_urls_to_parquet(spark_session=spark_session,
                                                                   targets=TARGETS, 
                                                                   schema=schema, 
                                                                   wet_urls=wet_urls, 
                                                                   parquet_name=parquet_name,
                                                                    pas=pas
                                                                   )
    # Il faut écrire dans un fichier sys.argv[1]_sys.argv[2]
    # Il faut écrire dans un autre fichier sys.argv[1]_sys.argv[2];n_total_file;n_total_no_occurence_found
    print(INFO + f"Pour {n_total_file} fichiers il y en a {n_total_no_occurence_found} ne comportant aucune occurence dans {TARGETS}")
    spark_session.stop()
    with open("wet_urls_downloaded", "a") as f:
        f.write(downloaded_name + "\n")

    extra_info = downloaded_name + ";" + str(n_total_file) + ";" + str(n_total_no_occurence_found) 
    with open("wet_parquet_extra_info", "a") as f:
        f.write(extra_info + "\n")


def main() : 

    spark_session = SparkSession.builder.config("spark.ui.port", sys.argv[4]).getOrCreate()

    if not os.path.exists("wet_urls_downloaded"):
        with open("wet_urls_downloaded", "w") as f:
            pass  

    downloaded_name = f"{sys.argv[1]}_{sys.argv[2]}"
    with open("wet_urls_downloaded","r") as f :
        for line in f:
            if line.strip() == downloaded_name:
                print(INFO + "url déjà téléchargé")
                return 
    first_url = int(sys.argv[1]) 
    last_url = int(sys.argv[2])
    pas = int(sys.argv[3])
    write_wet_parquet_files(
                            spark_session=spark_session,
                            downloaded_name=downloaded_name,
                            first_url=first_url,
                            last_url=last_url,
                            pas=pas
                            )

if __name__ == "__main__" : 
    main()

# 10 urls par minute sachant que j'ai 100000 urls par segment et j'ai plus d'un million d'urls au total pour 3 ans 