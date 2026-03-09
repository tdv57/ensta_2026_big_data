import requests
import os 
import gzip
from CC_name import get_CC_names
from LOG_MESSAGE import DEBUG, INFO, WARNING, ERROR

CC_url = "https://data.commoncrawl.org/"


def download_warc_paths(CC_archive_name):
        warc_paths_url = CC_url + f"crawl-data/{CC_archive_name}/warc.paths.gz"
        print(INFO + f"requesting the url {warc_paths_url}")
        DIRECTORY = "warc_paths_gz"

        os.makedirs(DIRECTORY, exist_ok=True)
        local_file = os.path.join(DIRECTORY, "warc.paths.gz")

        response = requests.get(warc_paths_url, stream=True)
        if response.status_code != 200:
            print(WARNING + f"response status has responded with code {response.status_code}")
        else : 
            print(INFO + "response status is 200")
        with open(local_file, "wb") as f : 
            for chunk in response.iter_content(8192):
                f.write(chunk)
        print(INFO + f"Fichier téléchargé : {local_file}")

        #########################################################################################################
        #########################################################################################################
        #####################   2ème partie où on lit les fichiers  #############################################
        #########################################################################################################
        #########################################################################################################

        SOURCE_DIRECTORY = "warc_paths_gz"
        DESTINATION_DIRECTORY = "warc_paths"
        DESTINATION_FILE =f"warc_paths_{CC_archive_name}.txt"
        os.makedirs(DESTINATION_DIRECTORY, exist_ok=True)

        print(INFO + f"Lecture du fichier {local_file}")

        with gzip.open(local_file, "rb") as fgz:
            warc_paths = [line.decode("utf-8").strip() for line in fgz]

        PATH_TO_FILE = DESTINATION_DIRECTORY + "/" + DESTINATION_FILE
        print(INFO + f"writing in file {PATH_TO_FILE}")


        with open(PATH_TO_FILE, "w") as f: 
            for path in warc_paths:
                f.write(path + "\n")

def main():
    CC_archive_names = get_CC_names(min_year=2023)
    for CC_archive_name in CC_archive_names:
        download_warc_paths(CC_archive_name)

if __name__ == "__main__":
    main()