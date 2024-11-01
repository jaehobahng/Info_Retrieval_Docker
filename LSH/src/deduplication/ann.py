import os
import pymongo
from collections import defaultdict
from utils.utils import clean_document, shingle, minhash
import hashlib

# Read MongoDB connection details from environment variables
mongo_host = os.getenv("MONGO_HOST", "localhost")
mongo_port = int(os.getenv("MONGO_PORT", "27017"))

# Connect to MongoDB
client = pymongo.MongoClient(f"mongodb://{mongo_host}:{mongo_port}")
db = client['data_db']  # Connect to the database

# Function to fetch data from a specific collection
def fetch_data_from_collection(collection_name):
    collection = db[collection_name]
    documents = collection.find()
    # for document in documents:
    #     print(document)
    result_dict = {document['_id']: document['text'] for document in documents}
    return result_dict

data_dict = fetch_data_from_collection('hundred')

# Function to fetch the LSH index from MongoDB and reconstruct it
def fetch_index_from_mongodb(index_name):
    collection = db[index_name]
    documents = collection.find()  # Retrieve all documents in the collection

    # Reconstruct the original defaultdict format
    reconstructed_index = defaultdict(list)

    for document in documents:
        index = document["index"]
        tuple_key = str(document["tuple_key"])  # Convert list back to tuple
        values = document["values"]
        reconstructed_index[(index, tuple_key)] = values

    return reconstructed_index


def get_minhash_signature(text):
    """Generate a MinHash signature for a single input text."""
    cleaned_text = clean_document(text)
    shingles = shingle(cleaned_text, k = k)
    return minhash(shingles, num_hashes=num_hashes)


def find_candidates_for_text(index, text):
    """Find candidate pairs for an input text by computing its MinHash signature and applying banding."""
    text_signature = get_minhash_signature(text)
    candidate_docs = set()

    # Apply banding on the text signature
    for band_idx in range(num_bands):
        start = band_idx * rows_per_band
        band = tuple(text_signature[start:start + rows_per_band])
        band_hash = hashlib.md5(str(band).encode()).hexdigest()

        # Check if any documents share this band hash
        if (band_idx, band_hash) in index:
            candidate_docs.update(index[(band_idx, band_hash)])

    return candidate_docs


# Example usage
# Replace 'your_index_name' with the actual index collection name in MongoDB
data_dict = fetch_data_from_collection('hundredk')
index_name = "hundredk_index"  # Replace with your specific index name
reconstructed_index = fetch_index_from_mongodb(index_name)

num_hashes = 100
num_bands = 20
rows_per_band = 5
k = 10

text = data_dict[98]

candidates = find_candidates_for_text(reconstructed_index,text)
# print(candidates)

for i in candidates:
    print(i,data_dict[i])
    print("")