import requests
import json
import sqlite3
from urllib.parse import urlparse, quote
from pathlib import Path
from .wm_cdx_utils import get_cdx_records
from .list_of_valid_tlds import list_of_valid_tlds
import requests


def original_url_validator(url: str) -> bool:
    try:
        result = urlparse(url)
    except ValueError:
        return False
    
    # Add check for missing hostname
    if not result.hostname:
        return False
    
    # Check if the tld of the URL is valid
    hostname_parts = result.hostname.split('.')
    if len(hostname_parts) < 2:  # Need at least domain.tld
        return False
    
    tld = hostname_parts[-1]
    if tld not in list_of_valid_tlds:
        return False
    
    # check if more than one instances of "//" exists in the URL
    if url.count("//") > 1:
        return False
    
    # check if the URL contains any of the following characters: <, >, {, }, |, \, ^, ~, [, ], `
    invalid_chars = set('<>{}|\\^~[]`')
    if any(char in invalid_chars for char in url):
        return False
    
    # check if the URL contains spaces
    if ' ' in url:
        return False

    return True

def check_if_url_already_in_db(url: str) -> bool:
    url = quote(url, safe='')
    base_pastinternet_url = "http://localhost:8000/redirect/"
    check_url = f"{base_pastinternet_url}{url}"
    response = requests.head(check_url)
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return True

def preprocess_urls_from_json_file(file_path: str, cdx_params: dict, bypass_url_validation: bool = False):
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    # data validation checks
    for entry in data:
        # check first if the three necessary keys are present
        if not all(key in entry for key in ("url", "title", "description")):
            raise ValueError("Each entry must contain 'url', 'title', and 'description' keys.")

    if not bypass_url_validation:
        urls_failing_validation = [entry["url"] for entry in data if not original_url_validator(entry["url"])]
        if urls_failing_validation:
            print("The following URLs failed validation:")
            for url in urls_failing_validation:
                print(url)
            raise ValueError("One or more URLs failed validation. Please correct them and try again.")

    # If we reach this point, all URLs are valid
    print("URL validation passed.")

    # print the number of URLs in the file
    print(f"Number of URLs in the file: {len(data)}")

    # check for database file existence and create if not exists. The database file is in the same directory as the JSON file
    db_path = file_path.rsplit('.', 1)[0] + '.db'    
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        
        # if the table does not exist, create it
        # The table will have columns for url, title, description, page_number, cdx_data
        if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", ('urls',)).fetchone():
            cursor.execute('''
                CREATE TABLE urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL,
                    title TEXT,
                    description TEXT,
                    category TEXT,
                    page_number INTEGER DEFAULT 0,
                    cdx_data TEXT
                );
            ''')

            # insert the data into the table
            for entry in data:
                cursor.execute('''
                    INSERT INTO urls (url, title, description, category, page_number)
                    VALUES (?, ?, ?, ?, ?);
                ''', (entry["url"], entry["title"], entry["description"], entry["category"], entry.get("page_number", 0)))
                conn.commit()
        
        # get a list of rows that have cdx_data as NULL
        cursor.execute("SELECT id, url FROM urls WHERE cdx_data IS NULL;")
        rows = cursor.fetchall()

        # print the number of rows that need cdx_data
        print(f"Number of rows that need cdx_data: {len(rows)}")

        # for each row, get the cdx_data and update the row
        for row in rows:
            id, url = row
            url_in_db = check_if_url_already_in_db(url)
            if url_in_db:
                cdx_data = [{"note": "skip, already in database"}]
            else:
                cdx_data = get_cdx_records(url, **cdx_params)
            cursor.execute("UPDATE urls SET cdx_data = ? WHERE id = ?;", (json.dumps(cdx_data), id))
            conn.commit()
    
    finally:
        conn.close()