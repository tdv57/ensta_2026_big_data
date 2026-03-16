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

def wet_urls_to_parquet(spark_session, schema, wet_urls, targets, parquet_name) : 
    BATCH_SIZE = 1000000

    rows = []
    n_no_occurence_found = 0 
    n_file = 0
    print(INFO + "writing parquet file for wet files")
    for (n_url, wet_url) in enumerate(wet_urls):
        if n_url % 1000 != 0:
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

def main() : 
    TARGETS = [["trump"], ["harris"],["biden"]]

    if not os.path.exists("wet_urls_downloaded"):
        with open("wet_urls_downloaded", "w") as f:
            pass  

    downloaded_name = f"{sys.argv[1]}_{sys.argv[2]}"
    with open("wet_urls_downloaded","r") as f :
        for line in f:
            if line.strip() == downloaded_name:
                print(INFO + "url déjà téléchargé")
                return 

    spark_session = SparkSession.builder.config("spark.ui.port", sys.argv[3]).getOrCreate()

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

    parquet_name = f"wet_parquet_files_{int(sys.argv[1])}_{int(sys.argv[2])}"
    wet_urls = dwet.get_wet_urls()[int(sys.argv[1]):int(sys.argv[2])]
    print(INFO + f"Il y a au total {len(dwet.get_wet_urls())}") #2700000 urls entre 2026 et 2023
    print(INFO + f"Creating parquet files for target {TARGETS}")
    n_total_file, n_total_no_occurence_found = wet_urls_to_parquet(spark_session=spark_session,targets=TARGETS, schema=schema, wet_urls=wet_urls, parquet_name=parquet_name)
    # Il faut écrire dans un fichier sys.argv[1]_sys.argv[2]
    # Il faut écrire dans un autre fichier sys.argv[1]_sys.argv[2];n_total_file;n_total_no_occurence_found
    print(INFO + f"Pour {n_total_file} fichiers il y en a {n_total_no_occurence_found} ne comportant aucune occurence dans {TARGETS}")
    spark_session.stop()
    with open("wet_urls_downloaded", "a") as f:
        f.write(downloaded_name + "\n")

    extra_info = downloaded_name + ";" + str(n_total_file) + ";" + str(n_total_no_occurence_found) 
    with open("wet_parquet_extra_info", "a") as f:
        f.write(extra_info + "\n")

if __name__ == "__main__" : 
    main()

# 10 urls par minute sachant que j'ai 100000 urls par segment et j'ai plus d'un million d'urls au total pour 3 ans 