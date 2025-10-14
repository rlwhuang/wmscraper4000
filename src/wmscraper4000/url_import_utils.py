# General URL importer
# This script defines functions to import URLs into the MongoDB database

from pymongo import MongoClient
from urllib.parse import urlparse
import wayback

class URLImporter:
    """Context manager for URL importing with reusable MongoDB connection."""
    
    def __init__(self, mongo_uri, database_name="xm", collection_name="urls_international", snapshot_collection_name="url_snapshots"):
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.snapshot_collection_name = snapshot_collection_name
        self.client = None
        self.collection = None
        self.snapshot_collection = None
        self.wayback_client = None
    
    def __enter__(self):
        self.client = MongoClient(self.mongo_uri)
        self.collection = self.client[self.database_name][self.collection_name]
        self.snapshot_collection = self.client[self.database_name][self.snapshot_collection_name]
        print("Connected to MongoDB")
        # print the number of documents in the collection
        print(f"Number of documents in collection '{self.collection_name}': {self.collection.count_documents({})}")
        print(f"Number of documents in collection '{self.snapshot_collection_name}': {self.snapshot_collection.count_documents({})}")
        self.wayback_client = wayback.WaybackClient()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()
    
    def add_url(self, url, lot_id, site_title, site_desc="", lot_path="", lot_path_code="", page_number=""):
        """Add a URL to the database."""
        netloc = urlparse(url).netloc
        url = url.replace(netloc, netloc.lower())

        print("Adding URL: " + url)

        lot_info = {
            "lot_id": lot_id,
            "site_title": site_title,
            "site_desc": site_desc,
        }

        if lot_path_code != "":
            lot_info["lot_path_code"] = lot_path_code

        if lot_path != "":
            lot_info["lot_path"] = lot_path

        if page_number != "":
            lot_info["page_number"] = page_number

        # query if url already in database
        query = self.collection.find_one({"url": url})

        if query is None:
            self.collection.insert_one({
                "url": url,
                "in_lots": [lot_info],
            })
            print("Added URL: " + url)
        else:
            lot_path_query = self.collection.find_one({"url": url, "in_lots.lot_id": lot_id, "in_lots.lot_path": lot_path})
            lot_path_code_query = self.collection.find_one({"url": url, "in_lots.lot_id": lot_id, "in_lots.lot_path_code": lot_path_code})
            if lot_path_query is not None or lot_path_code_query is not None:
                print("URL already in database: " + url)
            else: 
                self.collection.update_one({"url": url}, {"$push": {"in_lots": lot_info}})
                print("Updated URL: " + url)

    def add_url_snapshots(self, url, snapshots, force_update = False):
        """Add snapshots to an existing URL in the database."""
        
        print("Adding snapshots to URL: " + url)

        # verify if each element in snapshots is a dictionary with keys 'urlkey', 'timestamp', 'original', 'mimetype', 'statuscode', 'digest', 'length'
        for snapshot in snapshots:
            if not isinstance(snapshot, dict):
                raise ValueError("Each snapshot must be a dictionary")
            required_keys = ['urlkey', 'timestamp', 'original', 'mimetype', 'statuscode', 'digest', 'length']
            for key in required_keys:
                if key not in snapshot:
                    raise ValueError(f"Snapshot is missing required key: {key}")

        query = self.snapshot_collection.find_one({"url": url})
        if query is None:
            self.snapshot_collection.insert_one({
                "url": url,
                "wayback_cdx": snapshots,
            })
            print("Added snapshots to URL: " + url)
        elif force_update:
            self.snapshot_collection.update_one({"url": url}, {"$set": {"wayback_cdx": snapshots}})
            print("Force updated snapshots for URL: " + url)
        else:
            print("Snapshots already exist for URL: " + url)

    def get_unique_url_snapshots(self, url: str, from_date: int = 19960101000000, to_date: int = 20051231000000, status_code_filter: list = [200]) -> dict:
        if len(str(from_date)) != 14 or len(str(to_date)) != 14:
            raise ValueError("from_date and to_date must be in YYYYMMDDhhmmss format")
        
        # ensure status_code_filter is a list of integers
        if not all(isinstance(code, int) for code in status_code_filter):
            raise ValueError("status_code_filter must be a list of integers")

        # get all snapshots for the url from the database
        snapshots = self.snapshot_collection.find_one({"url": url})
        # get urlkey from the first snapshot
        if snapshots is not None:
            urlkey = snapshots.get("wayback_cdx", [])[0].get("urlkey", "") if snapshots else ""
            if not urlkey:
                raise ValueError("No urlkey found for URL: " + url)
            # Filter snapshots by date range
            filtered_snapshots = [
                snapshot for snapshot in snapshots.get("wayback_cdx", [])
                if from_date <= snapshot.get("timestamp", "") <= to_date and int(snapshot.get("statuscode", 0)) in status_code_filter
            ]
            
            # build a dictionary of digest keys to snapshots
            # format: digest_key: [snapshot_timestamp1, snapshot_timestamp2, ...]
            digest_to_snapshot = {}
            for snapshot in filtered_snapshots:
                digest_key = snapshot.get("digest", "")
                if digest_key:
                    if digest_key not in digest_to_snapshot:
                        digest_to_snapshot[digest_key] = []
                    digest_to_snapshot[digest_key].append(snapshot["timestamp"])
            return {
                "url": url,
                "urlkey": urlkey,
                "digest_to_snapshot": digest_to_snapshot
            }
        else:
            raise ValueError("No snapshots found for URL: " + url)
        
    def download_unique_id_snapshots(self, url, from_date, to_date) -> list:
        unique_snapshots = self.get_unique_url_snapshots(url, from_date, to_date)
        urlkey = unique_snapshots["urlkey"]
        unique_snapshots = unique_snapshots["digest_to_snapshot"]
        downloaded_snapshots = []
        for digest, timestamps in unique_snapshots.items():
            # for each digest, we only need to download one snapshot. We will download the first timestamp in the list.
            timestamp = timestamps[0]
            try:
                snapshot_data = self.wayback_client.get_memento(url, str(timestamp))
                downloaded_snapshots.append({
                    "urlkey": urlkey,
                    "url": url,
                    "digest": digest,
                    "timestamps": timestamps,
                    "data": snapshot_data
                })
            except Exception as e:
                print(f"Error downloading snapshot for URL: {url} at timestamp: {timestamp}. Error: {e}")
        return downloaded_snapshots