import os
import pymongo
from collections import defaultdict

# MongoDB connection
mongo_host = os.getenv("MONGO_HOST", "localhost")
mongo_port = int(os.getenv("MONGO_PORT", "27017"))
client = pymongo.MongoClient(f"mongodb://{mongo_host}:{mongo_port}")
db = client['data_db']


def load_tsv_files():
    data_dir = '/data'
    for filename in os.listdir(data_dir):
        if filename.endswith(".tsv"):
            collection_name = os.path.splitext(filename)[0]
            collection = db[collection_name]

            # Check if the collection already has data
            if collection.estimated_document_count() > 0:
                print(f"Skipping {filename}, collection '{collection_name}' already has data.")
                continue

            print(f"Loading data from {filename} into collection '{collection_name}'")

            # Batch insert documents
            batch = []
            with open(os.path.join(data_dir, filename), 'r') as file:
                for line in file:
                    index, text = line.strip().split('\t')
                    # document = {"index": int(index), "text": text}
                    document = {"_id": int(index), "text": text}
                    batch.append(document)
                    # Insert in batches of 1000
                    if len(batch) >= 1000:
                        collection.insert_many(batch, ordered=False)
                        batch = []

                # Insert any remaining documents
                if batch:
                    collection.insert_many(batch)
            print(f"Data from {filename} loaded into collection '{collection_name}'.")


if __name__ == "__main__":
    load_tsv_files()
    # load_defaultdict()
