# General URL importer
# This script defines functions to import URLs into the MongoDB database

from pymongo import MongoClient
from urllib.parse import urlparse

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
    
    def __enter__(self):
        self.client = MongoClient(self.mongo_uri)
        self.collection = self.client[self.database_name][self.collection_name]
        self.snapshot_collection = self.client[self.database_name][self.snapshot_collection_name]
        print("Connected to MongoDB")
        # print the number of documents in the collection
        print(f"Number of documents in collection '{self.collection_name}': {self.collection.count_documents({})}")
        print(f"Number of documents in collection '{self.snapshot_collection_name}': {self.snapshot_collection.count_documents({})}")
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

    def add_url_snapshots(url, snapshots: list, force_update=False):
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