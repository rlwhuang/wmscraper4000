import requests
from urllib.parse import quote
import time

def get_cdx_records(original_url: str, from_date: str = None, to_date: str = None, filter: str = None, sleep: float = 1.5) -> list:
    base = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url": original_url,
        "from": from_date,
        "to": to_date,
        "filter": filter,
    }

    # remove None values from params
    params = {k: v for k, v in params.items() if v is not None}

    # request the CDX records from the server
    time.sleep(sleep)  # be polite and avoid hammering the server
    response = requests.get(base, params=params)
    response.raise_for_status()

    # convert the response to a list of dictionaries
    records = []
    for line in response.text.strip().split("\n"):
        parts = line.split(" ")
        record = {
            "urlkey": parts[0],
            "timestamp": parts[1],
            "original": parts[2],
            "mimetype": parts[3],
            "statuscode": parts[4],
            "digest": parts[5],
            "length": parts[6],
        }
        records.append(record)
    
    return records

if __name__ == "__main__":
    # Example usage
    url = "mrshow.com"
    records = get_cdx_records(url, from_date="19960101", to_date="20051231")
    from pprint import pprint
    pprint(records)