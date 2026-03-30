import requests 
import os
from pathlib import Path
from warcio.archiveiterator import ArchiveIterator
from LOG_MESSAGE import DEBUG, INFO, WARNING, ERROR
import download_wet as dwet
from google.cloud import storage

CC_url = "https://data.commoncrawl.org/"

def wet_to_wat_url(wet_url):
    return wet_url.replace("/wet/", "/wat/").replace(".warc.wet.gz", ".warc.wat.gz")

def get_wat_urls(wat_from_wet = True) :
    CC_wat_urls = []

    if wat_from_wet:
        CC_wet_urls = dwet.get_wet_urls()
        for wet_url in CC_wet_urls:
            CC_wat_urls.append(wet_to_wat_url(wet_url))
        return 
    else: 
        DIR_WITH_WAT_PATHS_FILES = Path("wat_paths")

        for wat_paths_files in DIR_WITH_WAT_PATHS_FILES.iterdir():
            if wat_paths_files.is_file():
                with open (wat_paths_files, "r") as f:
                    CC_wat_urls += f.read().split()
        return CC_wat_urls

def get_gcp_wat_urls(bucket_path, wat_from_wet= True) :
    CC_wat_urls = []

    if wat_from_wet:
        CC_wet_urls = dwet.get_gcp_wet_urls(bucket_path=bucket_path)
        for wet_url in CC_wet_urls:
            CC_wat_urls.append(wet_to_wat_url(wet_url))
        return CC_wat_urls
    bucket_name = bucket_path[5:]
    dir = "wat_paths"
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Lister les fichiers sous wet_paths
    blobs = bucket.list_blobs(prefix=dir)
    for blob in blobs:
        content = blob.download_as_text()
        CC_wat_urls += content.strip().split("\n")
        if CC_wat_urls == []:
            raise ValueError(ERROR + "get_gcp_wat_urls empty dans la bloucle blob")

    if CC_wat_urls == []:
        raise ValueError (ERROR + "get_gcp_wat_urls empty")
    return CC_wat_urls
    

# Normalement on va itérer sur les wat_paths compris dans le wat_paths.gz qu'on ne décompressera pas sur disque
def get_wat_response(CC_wat_url) : 
    wat_paths_url = CC_url + CC_wat_url
    print(INFO + f"requesting the url {wat_paths_url}")
    DIRECTORY = "wat_paths_gz"

    response = requests.get(wat_paths_url, stream=True)
    if response.status_code != 200:
        print(WARNING + f"response status has responded with code {response.status_code}")
        print(INFO + f"adding urls to the error file wat_urls_error")
        # On va écrire les urls qui ont planté dans un fichier d'erreur qu'on téléchargera plus tard
        with open("wat_urls_error", "a") as f :
            f.write(wat_paths_url + "\n")
        return None
    else : 
        print(INFO + "response status is 200")
    return response

def get_wat_page(response, page_wanted):
    stream = ArchiveIterator(response.raw)
    for (page_index, record) in enumerate(stream):
        if page_index == page_wanted:
            if record.rec_type == 'metadata':
                content = record.content_stream().read() 
                print(INFO + "contenu trouvé")
                return content.decode("utf-8", errors="replace")
            else : 
                raise ValueError ("record wanted is not a of type response")
        
def gcp_build_df_urls(spark_session, bucket_path,first_url, last_url, pas):
    wat_urls = get_gcp_wat_urls(bucket_path=bucket_path)[first_url:last_url:pas]
    if not wat_urls:
        print(f"Aucune URL trouvée entre {first_url} et {last_url}")
        raise ValueError (f"wet_urls vide first={first_url} last={last_url} pas={pas}")
    n_partitions = min(10, int(len(wat_urls)/100) + 1)
    df_urls = spark_session.createDataFrame(
        [(url,) for url in wat_urls],
        ["url"]
    ).repartition(n_partitions)  # ajuste selon cluster

    return df_urls 


def main():
    CC_wat_urls = get_wat_urls()
    print(INFO + f"il y a {len(CC_wat_urls)} url trouvées")
    CC_wat_url = CC_wat_urls[0]
    response = get_wat_response(CC_wat_url)
    page_wanted = 4
    page_content  = get_wat_page(response, page_wanted)
    print(INFO + f"Contenu de la {page_wanted} ème page du WAT :\n")  
    print(page_content)
    
if __name__ == "__main__":
    main()

