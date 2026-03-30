import run_wat
import run_wet
from LOG_MESSAGE import INFO, ERROR, WARNING, DEBUG
import sys 
import os 

def main():
    if not os.path.exists("wat_parquet"):
        os.mkdir("wat_parquet")
    if not os.path.exists("wet_parquet"):
        os.mkdir("wet_parquet")
    if not os.path.exists("wet_urls_downloaded"):
        open("wet_urls_downloaded", 'a').close()
    if not os.path.exists("wat_urls_downloaded"):
        open("wat_urls_downloaded", 'a').close()
    if not os.path.exists("wet_urls_error"):
        open("wet_urls_error", 'a').close()
    if not os.path.exists("wat_urls_error"):
        open("wat_urls_error", 'a').close()
    if not os.path.exists("wet_parquet_extra_info"):
        open("wet_parquet_extra_info", 'a').close()
    first_url = int(sys.argv[1])
    last_url = int(sys.argv[2])
    pas = int(sys.argv[3])
    port = int(sys.argv[4])
    print(INFO + "RUN pour écrire les fichiers WAT")
    run_wat.run_wat(first_url=first_url,
                    last_url=last_url,
                    pas=pas,
                    port=port)
    print(INFO + "RUN pour écrire les fichiers WET")
    run_wet.run_wet(first_url=first_url,
                    last_url=last_url,
                    pas=pas,
                    port=port)

if __name__ == "__main__":
    main()