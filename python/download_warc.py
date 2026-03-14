import requests 
import os
from pathlib import Path
from warcio.archiveiterator import ArchiveIterator
from LOG_MESSAGE import DEBUG, INFO, WARNING, ERROR

CC_url = "https://data.commoncrawl.org/"

def get_warc_urls() : 
    CC_warc_urls = []
    DIR_WITH_WARC_PATHS_FILES = Path("warc_paths")

    for warc_paths_files in DIR_WITH_WARC_PATHS_FILES.iterdir():
        if warc_paths_files.is_file():
            with open (warc_paths_files, "r") as f:
                CC_warc_urls += f.read().split()
    return CC_warc_urls


# Normalement on va itérer sur les warc_paths compris dans le warc_paths.gz qu'on ne décompressera pas sur disque


def get_warc_response(CC_warc_url):
    warc_paths_url = CC_url + CC_warc_url
    print(INFO + f"requesting the url {warc_paths_url}")
    DIRECTORY = "warc_paths_gz"
    response = requests.get(warc_paths_url, stream=True)
    if response.status_code != 200:
        print(WARNING + f"response status has responded with code {response.status_code}")
    else : 
        print(INFO + "response status is 200")
    return response

def get_warc_page(response,page_wanted):
    stream = ArchiveIterator(response.raw)
    for(page_index, record) in enumerate(stream):
        if page_index == page_wanted:
            if record.rec_type == 'response' or record.rec_type == 'request':
                content = record.content_stream().read() 
                print(INFO + "Contenu trouvé")
                return content.decode("utf-8", errors="replace")
            else : 
                raise ValueError (f"record wanted is not a of type response. It is of type {record.rec_type}")
        
def main():
    CC_warc_urls = get_warc_urls()
    print(INFO + f"il y a {len(CC_warc_urls)} url trouvées")
    CC_warc_url = CC_warc_urls[0]
    response = get_warc_response(CC_warc_url)
    page_wanted = 101
    page_content  = get_warc_page(response, page_wanted)
    print(INFO + f"Contenu de la {page_wanted} ème page du WARC :\n")  
    print(page_content)

if __name__ == "__main__":
    main()