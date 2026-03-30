import requests
from datetime import datetime
from LOG_MESSAGE import INFO

url = "https://index.commoncrawl.org/collinfo.json"

def get_CC_names(min_year, max_year):
    min_ref_date = datetime(min_year, 1, 1)
    max_ref_date = datetime(max_year, 1, 1)
    snapshot_ids = []
    snapshots = requests.get(url).json()
    for snapshot in snapshots:
        ref_date = datetime.strptime(snapshot["from"], "%Y-%m-%dT%H:%M:%S")
        if min_ref_date <= ref_date <= max_ref_date:
            print(INFO + f"append snapshot with id {snapshot['id']}")
            snapshot_ids.append(snapshot["id"])
    return snapshot_ids
