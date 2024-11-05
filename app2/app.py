from flask import Flask, request, jsonify, render_template
import os
import pymongo
from collections import defaultdict
from utils.utils import clean_document, shingle, minhash
import hashlib

# Initialize Flask app
app = Flask(__name__)

# MongoDB connection details
mongo_host = os.getenv("MONGO_HOST", "localhost")
mongo_port = int(os.getenv("MONGO_PORT", "27017"))

# Connect to MongoDB
client = pymongo.MongoClient(f"mongodb://{mongo_host}:{mongo_port}")
db = client['data_db']

# Parameters for LSH
num_hashes = 100
k = 10

# Load available index collections at startup
collections = db.list_collection_names()
filtered_collections = [name for name in collections if name.endswith('_index')]
filtered_signature = [name for name in collections if name.endswith('_signature')]
filtered_collections.sort()
filtered_signature.sort()

# Load the data dictionary and index from MongoDB at startup
data_dict = {}
index_name = "hundred_index"
reconstructed_index = defaultdict(list)

def load_data():
    global data_dict, reconstructed_index
    data_dict = fetch_data_from_collection('hundredk')
    reconstructed_index = fetch_index_from_mongodb(index_name)

# Fetch collection data from MongoDB
def fetch_data_from_collection(collection_name):
    collection = db[collection_name]
    documents = collection.find()
    return {document['_id']: document['text'] for document in documents}

# Fetch LSH index from MongoDB
def fetch_index_from_mongodb(index_name):
    collection = db[index_name]
    documents = collection.find()
    reconstructed_index = defaultdict(list)
    for document in documents:
        index = document["index"]
        tuple_key = str(document["tuple_key"])  # Convert list back to tuple
        values = document["values"]
        reconstructed_index[(index, tuple_key)] = values
    return reconstructed_index

def fetch_signature_from_mongodb(signature_name):
    collection = db[signature_name]
    documents = collection.find()
    reconstructed_signature = defaultdict(list)
    for document in documents:
        index = document["doc"]
        values = [int(i) for i in document["signature"]]
        reconstructed_signature[index] = values
    return reconstructed_signature


# Function to generate MinHash signature
def get_minhash_signature(text):
    cleaned_text = clean_document(text)
    shingles = shingle(cleaned_text, k)
    return minhash(shingles, num_hashes)


def get_index(signatures, num_bands, rows_per_band):
    index = defaultdict(list)

    for doc_id, signature in signatures.items():
        for band_idx in range(num_bands):
            start = band_idx * rows_per_band
            band = tuple(signature[start:start + rows_per_band])  # Convert band into a tuple for hashing
            band_hash = hashlib.md5(str(band).encode()).hexdigest()  # Hash the band

            # Store doc_id in the index for this band
            index[(band_idx, band_hash)].append(doc_id)
    return index

# Function to find candidates for input text
def find_candidates_for_text(index, text, num_bands, rows_per_band):
    text_signature = get_minhash_signature(text)
    candidate_docs = set()
    for band_idx in range(num_bands):
        start = band_idx * rows_per_band
        band = tuple(text_signature[start:start + rows_per_band])
        band_hash = hashlib.md5(str(band).encode()).hexdigest()
        if (band_idx, band_hash) in index:
            candidate_docs.update(index[(band_idx, band_hash)])
    return candidate_docs

# Flask route for the web interface
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        input_text = request.form.get('input_text')
        selected_index = request.form.get('selected_index')
        num_bands = int(request.form.get('num_bands', 20))  # Default to 20 if not provided
        rows_per_band = 100 // num_bands  # Calculate rows_per_band dynamically


        # Fetch the data dictionary and index based on the selected index
        data_dict = fetch_data_from_collection(selected_index.replace('_signature', ''))
        reconstructed_signature = fetch_signature_from_mongodb(selected_index)
        reconstructed_index = get_index(reconstructed_signature, num_bands, rows_per_band)
        
        candidates = find_candidates_for_text(reconstructed_index, input_text, num_bands, rows_per_band)
        results = [(i, data_dict[i]) for i in candidates]

        return render_template('results.html', results=results, input_text=input_text, selected_index=selected_index)
    
    divisors_of_100 = [i for i in range(1, 101) if 100 % i == 0]
    
    return render_template('index.html', filtered_signature=filtered_signature, divisors_of_100=divisors_of_100)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
