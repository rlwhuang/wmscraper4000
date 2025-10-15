import tenacity
import requests
import time
from urllib.parse import quote

retry = tenacity.retry(
    stop=tenacity.stop_after_attempt(12), 
    wait=tenacity.wait_exponential(multiplier=1, min=32, max=64)
)

@retry
def download_archived_snapshot(original_url, timestamp, rewrite_modifier="id_", sleep=1, use_apparent_encoding=True):
    original_url = quote(original_url, safe="")
    snapshot_url = f"https://web.archive.org/web/{timestamp}{rewrite_modifier}/{original_url}"
    print(f"Fetching archived snapshot for: {snapshot_url}")
    response = requests.get(snapshot_url, allow_redirects=True)
    print(f"Received response with status code: {response.status_code}")
    time.sleep(sleep)

    if response.status_code in [404, 403]: 
        return {
            "status_code": response.status_code,
            "payload": None,
            "headers": dict(response.headers)
        }
    response.raise_for_status()
    
    content_type = response.headers.get("Content-Type")

    if content_type and content_type.startswith("text"):
        if use_apparent_encoding:
            response.encoding = response.apparent_encoding
        return {
            "status_code": response.status_code,
            "payload": response.text,
            "headers": dict(response.headers)
        }
    else:
        return {
            "status_code": response.status_code,
            "payload": response.content,
            "headers": dict(response.headers)
        }