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


# Function to fetch data from a specific collection
def fetch_data_from_collection(collection_name):
    collection = db[collection_name]
    documents = collection.find()
    # for document in documents:
    #     print(document)
    result_dict = {document['_id']: document['text'] for document in documents}
    return result_dict

for i in filtered_collections:
    
    index_name = i + "_index"
    collection = db[index_name]
    # collection.delete_many({})

    # Check if the index collection already has data
    if collection.count_documents({}) > 0:
        print(f"{index_name} already contains data, skipping...")
        continue  # Skip to the next collection if data is already present

    print(f"Processing collection: {i}")

    data_dict = fetch_data_from_collection(i)

    lsh = LSH(num_hashes=100, num_bands=20, rows_per_band=5, k=10)
    signatures = lsh.compute_minhash_signatures(data_dict)

    # Initialize LSH with the signature DataFrame

    # Step 1: Band the existing data to find candidate pairs
    candidate_pairs = lsh.banding()
    # print("Candidate Pairs:", candidate_pairs)

    # print("Signature Done")

    print(f"Insert to MongoDB : {index_name}")
    
    documents = []
    count = 0

    for key, value in lsh.index.items():
        document = {
            "index": key[0],
            "tuple_key": key[1],  # Storing the tuple as a list in MongoDB
            "values": value
        }
        documents.append(document)
        count += 1

        # If the batch size is reached, insert and reset the documents list
        if count % 10000 == 0:
            collection.insert_many(documents)
            documents = []  # Clear the batch list after insertion

    # Insert any remaining documents in the final batch
    if documents:
        collection.insert_many(documents)

    print(f"All {index_name} signatures stored in MongoDB.")