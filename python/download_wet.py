import requests 
import os
from pathlib import Path
from warcio.archiveiterator import ArchiveIterator
from LOG_MESSAGE import DEBUG, INFO, WARNING, ERROR
import sys 
from google.cloud import storage

CC_url = "https://data.commoncrawl.org/"

def get_wet_urls() : 
    CC_wet_urls = []
    DIR_WITH_wet_PATHS_FILES = Path("wet_paths")

    for wet_paths_files in DIR_WITH_wet_PATHS_FILES.iterdir():
        if wet_paths_files.is_file():
            with open (wet_paths_files, "r") as f:
                CC_wet_urls += f.read().split()
    return CC_wet_urls

def get_gcp_wet_urls(bucket_path) :

    CC_wet_urls = [] 
    bucket_name = bucket_path[5:]
    dir = "wet_paths"
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Lister les fichiers sous wet_paths
    blobs = bucket.list_blobs(prefix=dir)
    for blob in blobs:
        content = blob.download_as_text()
        CC_wet_urls += content.strip().split("\n")
        if CC_wet_urls == []:
            raise ValueError(ERROR + "get_gcp_wet_urls empty dans la bloucle blob")

    if CC_wet_urls == []:
        raise ValueError (ERROR + "get_gcp_wet_urls empty")
    return CC_wet_urls
    
def get_wet_response(CC_wet_url) : 
# Normalement on va itérer sur les wet_paths compris dans le wet_paths.gz qu'on ne décompressera pas sur disque
    wet_paths_url = CC_url + CC_wet_url
    print(INFO + f"requesting the url {wet_paths_url}")
    DIRECTORY = "wet_paths_gz"

    response = requests.get(wet_paths_url, stream=True)
    if response.status_code != 200:
        print(WARNING + f"response status has responded with code {response.status_code}")
        print(INFO + f"adding urls to the error file wet_urls_error")
        # On va écrire les urls qui ont planté dans un fichier d'erreur qu'on téléchargera plus tard
        with open("wet_urls_error", "a") as f :
            f.write(wet_paths_url + "\n")
        return None
    else : 
        print(INFO + "response status is 200")
    return response 

def get_wet_page(response, page_wanted):
    stream = ArchiveIterator(response.raw)
    for (page_index, record) in enumerate(stream):
        if page_index == page_wanted:
            if record.rec_type == 'conversion':
                content = record.content_stream().read() 
                print(INFO + "contenu trouvé")
                return content.decode("utf-8", errors="replace")
            else : 
                raise ValueError ("record wanted is not a of type response")

def gcp_build_df_urls(spark_session, bucket_path,first_url, last_url, pas):
    wet_urls = get_gcp_wet_urls(bucket_path)[first_url:last_url:pas]
    if not wet_urls:
        print(f"Aucune URL trouvée entre {first_url} et {last_url}")
        raise ValueError (f"wet_urls vide first={first_url} last={last_url} pas={pas}")
    n_partitions = min(10, int(len(wet_urls)/100) + 1)
    df_urls = spark_session.createDataFrame(
        [(url,) for url in wet_urls],
        ["url"]
    ).repartition(n_partitions)  # ajuste selon cluster

    return df_urls 

def main():
    CC_wet_urls = get_wet_urls()
    print(INFO + f"il y a {len(CC_wet_urls)} url trouvées")
    CC_wet_url = CC_wet_urls[0]
    response = get_wet_response(CC_wet_url)
    page_wanted = 1
    page_content  = get_wet_page(response, page_wanted)
    print(INFO + f"Contenu de la {page_wanted} ème page du WET :\n")  
    print(page_content)
    
if __name__ == "__main__":
    main()