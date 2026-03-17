import run_wat
import run_wet
from LOG_MESSAGE import INFO, ERROR, WARNING, DEBUG
import sys 

def main():
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