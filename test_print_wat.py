from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from LOG_MESSAGE import INFO, DEBUG, ERROR, WARNING
import download_warc as dwarc
import download_wat as dwat 
import download_wet as dwet 
from warcio.archiveiterator import ArchiveIterator
from datetime import datetime
import time
import json 
import subprocess
import sys 
from urllib.parse import urlparse

def extract_host_and_path(url):

    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path + ('?' + parsed.query if parsed.query else '')
    
    return host, path

def main():
    wat_url = dwat.get_wat_urls()[int(sys.argv[1])]
    wat_response = dwat.get_wat_response(wat_url)
    wat_stream = ArchiveIterator(wat_response.raw)
        
    for (index,record) in enumerate(wat_stream) :
        if index == int(sys.argv[2]): 
            metadata = record.content_stream().read().decode("utf-8", errors="ignore") 
            result = subprocess.run(
                ["jq"],
                input=metadata,
                text=True,
                capture_output=True
            )

            print(result.stdout)
            json_metadata = json.loads(metadata)
            

            uri = json_metadata["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
            warc_record_id  = json_metadata["Envelope"]["WARC-Header-Metadata"]["WARC-Record-ID"]
            print("URI:", uri)
            print("Warc-Record-id:", warc_record_id)
            payload_metadata = json_metadata["Envelope"]["Payload-Metadata"]
            if "HTTP-Request-Metadata" in payload_metadata:
                host = payload_metadata["HTTP-Request-Metadata"]["Headers"]["Host"]
                path = payload_metadata["HTTP-Request-Metadata"]["Request-Message"]["Path"]

                print("Host:", host)
                print("Path:", path)
            elif "HTTP-Response-Metadata" in payload_metadata:
                    headers = payload_metadata["HTTP-Response-Metadata"]["Headers"]
                    title =  payload_metadata["HTTP-Response-Metadata"]["HTML-Metadata"]["Head"]["Title"]
                    status = payload_metadata["HTTP-Response-Metadata"]["Response-Message"].get("Status", None)
                    host, path = extract_host_and_path(uri)
                    print("Headers:", headers)
                    print("Status:", status)
                    print("Title:", title)
                    print("Host:", host)
                    print("Path:", path)

if __name__ == "__main__":
    main()
