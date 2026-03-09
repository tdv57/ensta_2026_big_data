import requests 
import os
from pathlib import Path
from warcio.archiveiterator import ArchiveIterator
from LOG_MESSAGE import DEBUG, INFO, WARNING, ERROR

CC_url = "https://data.commoncrawl.org/"



def get_wat_urls() : 
    CC_wat_urls = []
    DIR_WITH_WAT_PATHS_FILES = Path("wat_paths")

    for wat_paths_files in DIR_WITH_WAT_PATHS_FILES.iterdir():
        if wat_paths_files.is_file():
            with open (wat_paths_files, "r") as f:
                CC_wat_urls += f.read().split()
    return CC_wat_urls


# Normalement on va itérer sur les wat_paths compris dans le wat_paths.gz qu'on ne décompressera pas sur disque
def get_wat_response(CC_wat_url) : 
    wat_paths_url = CC_url + CC_wat_url
    print(INFO + f"requesting the url {wat_paths_url}")
    DIRECTORY = "wat_paths_gz"

    response = requests.get(wat_paths_url, stream=True)
    if response.status_code != 200:
        print(WARNING + f"response status has responded with code {response.status_code}")
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

