import os
import pymongo
from collections import defaultdict

# MongoDB connection
mongo_host = os.getenv("MONGO_HOST", "localhost")
mongo_port = int(os.getenv("MONGO_PORT", "27017"))
client = pymongo.MongoClient(f"mongodb://{mongo_host}:{mongo_port}")
db = client['data_db']

# def load_tsv_files():
#     # List all TSV files in the /data directory
#     data_dir = './'
#     for filename in os.listdir(data_dir):
#         if filename.endswith(".tsv"):
#             collection_name = os.path.splitext(filename)[0]  # Use filename as collection name
#             collection = db[collection_name]
#             collection.delete_many({})  # Clear existing data in the collection
            
#             # Load data from each TSV file into the corresponding collection
#             with open(os.path.join(data_dir, filename), 'r') as file:
#                 for line in file:
#                     index, text = line.strip().split('\t')
#                     document = {"index": int(index), "text": text}
#                     collection.insert_one(document)
#             print(f"Data from {filename} loaded into collection '{collection_name}'.")

# def load_tsv_files():
#     data_dir = '/data'
#     for filename in os.listdir(data_dir):
#         if filename.endswith(".tsv"):
#             collection_name = os.path.splitext(filename)[0]
#             collection = db[collection_name]

#             # Check if the collection already has data
#             if collection.estimated_document_count() > 0:
#                 print(f"Skipping {filename}, collection '{collection_name}' already has data.")
#                 continue

#             print(f"Loading data from {filename} into collection '{collection_name}'")
            
#             # Batch insert documents
#             batch = []
#             with open(os.path.join(data_dir, filename), 'r') as file:
#                 for line in file:
#                     index, text = line.strip().split('\t')
#                     document = {"index": int(index), "text": text}
#                     batch.append(document)
#                     if len(batch) >= 1000:
#                         collection.insert_many(batch)
#                         batch = []
                
#                 # Insert any remaining documents
#                 if batch:
#                     collection.insert_many(batch)
#             print(f"Data from {filename} loaded into collection '{collection_name}'.")

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



# def load_defaultdict():
#     collection = db['defaultdict_data']
#     collection.delete_many({})  # Clear existing data

#     # Example defaultdict data (replace with actual data if needed)
#     data = defaultdict(list, {
#         (0, (14018493315494469, 8928069337884594)): [1],
#         (1, (23886939199836151, 31479077789304207)): [1]
#     })

#     # Convert defaultdict to a list of documents for MongoDB
#     documents = [
#         {"key": key[0], "tuple_key": key[1], "values": values}
#         for key, values in data.items()
#     ]
#     collection.insert_many(documents)
#     print("Defaultdict data loaded into MongoDB.")

if __name__ == "__main__":
    load_tsv_files()
    # load_defaultdict()
