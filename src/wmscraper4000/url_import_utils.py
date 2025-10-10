import requests
import json
import sqlite3
from urllib.parse import urlparse
from pathlib import Path
from wm_cdx_utils import get_cdx_records

def preprocess_urls_from_json_file(file_path: str) -> list:
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    # data validation checks
    for entry in data:
        # check first if the three necessary keys are present
        if not all(key in entry for key in ("url", "title", "description")):
            raise ValueError("Each entry must contain 'url', 'title', and 'description' keys.")
        
        # check if all URLs are valid URLs
        parsed_url = urlparse(entry["url"])
        if not all([parsed_url.scheme, parsed_url.netloc]):
            raise ValueError(f"Invalid URL found: {entry['url']}")
        
        # check if all URLs start from http (there should be no other protocols)
        if not parsed_url.scheme.startswith("http"):
            raise ValueError(f"URL must start with http or https: {entry['url']}")
    
    # check for database file existence and create if not exists. The database file is in the same directory as the JSON file
    db_path = file_path.rsplit('.', 1)[0] + '.db'    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # if the table does not exist, create it
    # The table will have columns for url, title, description, page_number, cdx_data
    if not cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls';").fetchone():
        cursor.execute('''
            CREATE TABLE urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                title TEXT,
                description TEXT,
                page_number INTEGER DEFAULT 0,
                cdx_data TEXT
            );
        ''')

        # insert the data into the table
        for entry in data:
            cursor.execute('''
                INSERT INTO urls (url, title, description)
                VALUES (?, ?, ?);
            ''', (entry["url"], entry["title"], entry["description"]))
            conn.commit()
    
    # get a list of rows that have cdx_data as NULL
    cursor.execute("SELECT id, url FROM urls WHERE cdx_data IS NULL;")
    rows = cursor.fetchall()
    
    # for each row, get the cdx_data and update the row
    for row in rows:
        id, url = row
        cdx_data = get_cdx_records(url)
        cursor.execute("UPDATE urls SET cdx_data = ? WHERE id = ?;", (json.dumps(cdx_data), id))
        conn.commit()
    
    # close the connection
    conn.close()
    
    