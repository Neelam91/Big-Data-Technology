from requests.auth import HTTPBasicAuth
import requests
from sodapy import Socrata
import sys
import os
import argparse
from datetime import datetime

#Creating Argparse CLI argument
parser = argparse.ArgumentParser(description='Process data from opcv.')
parser.add_argument('--page_size', type=int,
                    help='how many rows to get per page', required=True)
parser.add_argument('--num_pages',
                    type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])

DATASET_ID = os.environ["DATASET_ID"]
APP_TOKEN = os.environ["APP_TOKEN"]
ES_HOST = os.environ["ES_HOST"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]

#connecting to elastic search  
resp = requests.get(ES_HOST, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD))
print(resp.json())

#giving an index name in elastic search
if __name__ == '__main__':
    try:
        resp = requests.put(f"{ES_HOST}/opcv2", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                 }, "mappings": {
                  "properties": {
                        "plate": {"type": "text"},
                        "state": {"type": "keyword"},
                        "license_type": {"type": "text"},
                        "summons_number": {"type": "float"},
                        "issue_date": {"type": "date", "format":"yyyy-MM-dd"},
                        "violation-time":{"type": "keyword"},
                        "violation": {"type": "text"},
                        "fine_amount": {"type": "float"},
                        "penalty_amount": {"type": "float"},
                        "interest_amount": {"type": "float"},
                        "reduction_amount": {"type": "float"},
                        "payment_amount": {"type": "float"},
                        "amount_due": {"type": "float"},
                        "precinct": {"type": "float"},
                        "county": {"type": "keyword"},
                        "issuing_agency": {"type": "text"},
                    }
                },
            })
        resp.raise_for_status()
        print(r.json())
    except Exception:
        print("Index already exists! Skipping")
# calling the API and getting the data
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout = 50000)
    #rows = client.get(DATASET_ID, limit=args.page_size)
    page_size = args.page_size
    num_pages = args.num_pages
    
    if num_pages is None:
        total_rows = client.get(DATASET_ID, select='COUNT(*)')
        total = int(total_rows[0]['COUNT'])
        num_pages= int(total/page_size)

        #print (f'num_pages = {num_pages}')
        #print (f'page_size = {page_size}')
        
    for i in range(num_pages):
        rows = client.get(DATASET_ID, limit=args.page_size, offset = i*page_size)
        #print(rows)
        for row in rows:
            try:
                #convert
                row["summons_number"] = int(row.get("summons_number"))
                row["fine_amount"] = float(row.get("fine_amount"))
                row["penalty_amount"] = float(row.get("penalty_amount"))
                row["interest_amount"] = float(row.get("interest_amount"))
                row["reduction_amount"] = float(row.get("reduction_amount"))
                row["payment_amount"] = float(row.get("payment_amount"))
                row["amount_due"] = float(row.get("amount_due"))
                if "issue_date" in row:
                    row["issue_date"] = datetime.strptime(row["issue_date"],"%M/%d/%Y").strftime("%Y-%M-%d")
                del row["summons_image"]
                #out_dictionary[count] = row
            except Exception as e:
                print(f"Error!: {e}, skipping row: {row}")
                continue
    #print(out_dictionary)
# uploading document to elasticsearch index opcv
            try:
                #this is the URL to create a new traffic document
                #which is our "row" in elasticsearch databse/table
                resp = requests.post(f"{ES_HOST}/opcv2/_doc",
                json= row, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD))
                resp.raise_for_status()
            except Exception as e:
                print(f"Failed to insert in ES: {e}, skipping row: {row}")
                #continue
            print(resp.json())




