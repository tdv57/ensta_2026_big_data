import download_wat as dwat
import download_wet as dwet 
from LOG_MESSAGE import DEBUG, INFO
def main():
    wat_urls = dwat.get_wat_urls()
    wat_response = dwat.get_wat_response(wat_urls[0])
    wat_stream = (wat_response.raw)
    wet_urls = dwet.get_wet_urls()
    wet_response = dwet.get_wet_response(wet_urls[0])
    wet_stream =  (wet_response.raw)
    wet_count = 0 
    print(INFO + f"nombre de wat_urls = {len(wat_urls)}")
    print(INFO + f"nombre de wet_urls = {len(wet_urls)}")
    for record in wet_stream:
        wet_count += 1
    wat_count = 0 
    for record in wat_stream:
        wat_count += 1 
    print(f"len wat_stream = {wat_count}")
    print(f"len wet_stream = {wet_count}")

if __name__ == "__main__" :
    main()
