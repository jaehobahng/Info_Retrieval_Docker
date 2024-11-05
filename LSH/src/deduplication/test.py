import os
import pymongo

from deduplication.LSH import LSH
from utils.use_cases import collection_deduplication, nearest_neighbor_search
import pandas as pd

from collections import defaultdict
from utils.utils import UnionFind, clean_document, shingle, minhash

import hashlib

# Read MongoDB connection details from environment variables
mongo_host = os.getenv("MONGO_HOST", "localhost")
mongo_port = int(os.getenv("MONGO_PORT", "27017"))

# Connect to MongoDB
client = pymongo.MongoClient(f"mongodb://{mongo_host}:{mongo_port}")
db = client['data_db']  # Connect to the database


collections = db.list_collection_names()
filtered_collections = [name for name in collections if (not name.endswith('_index') and name != 'five')]  # Filter out too short



collection = db["hundred_signature"]

# Drop the collection
collection.drop()

collection = db["onek_signature"]

# Drop the collection
collection.drop()


# Function to fetch data from a specific collection
def fetch_data_from_collection(collection_name):
    collection = db[collection_name]
    documents = collection.find()
    # for document in documents:
    #     print(document)
    result_dict = {document['_id']: document['text'] for document in documents}
    return result_dict

filtered_collections = ['hundred','onek']

for i in filtered_collections:
    
    index_name = i + "_signature"
    collection = db[index_name]
    # collection.delete_many({})

    # Check if the index collection already has data
    if collection.count_documents({}) > 0:
        print(f"{index_name} already contains data, skipping...")
        continue  # Skip to the next collection if data is already present

    print(f"Processing collection: {index_name}")


    data_dict = fetch_data_from_collection(i)

    lsh = LSH(num_hashes=100, num_bands=20, rows_per_band=5, k=10)
    signatures = lsh.compute_minhash_signatures(data_dict)

    def convert_values_to_strings(data):
        if isinstance(data, dict):
            return {key: convert_values_to_strings(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [convert_values_to_strings(element) for element in data]
        else:
            return str(data)
        

    signatures = convert_values_to_strings(signatures)

    documents = []
    count = 0

    for key, value in signatures.items():
        document = {
            "doc": key,
            "signature": value
        }
        documents.append(document)
        count += 1

        # If the batch size is reached, insert and reset the documents list
        if count % 10000 == 0:
            collection.insert_many(documents)
            documents = []  # Clear the batch list after insertion
            print(f"{count} input in mongodb")

    # Insert any remaining documents in the final batch
    if documents:
        collection.insert_many(documents)

    print(f"All {index_name} signatures stored in MongoDB.")