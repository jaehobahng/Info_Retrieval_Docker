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
num_bands = 20
rows_per_band = 5
k = 10



# Load available index collections at startup
collections = db.list_collection_names()
filtered_collections = [name for name in collections if name.endswith('_index')]

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

# Function to generate MinHash signature
def get_minhash_signature(text):
    cleaned_text = clean_document(text)
    shingles = shingle(cleaned_text, k)
    return minhash(shingles, num_hashes)

# Function to find candidates for input text
def find_candidates_for_text(index, text):
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

        # Fetch the data dictionary and index based on the selected index
        data_dict = fetch_data_from_collection(selected_index.replace('_index', ''))
        reconstructed_index = fetch_index_from_mongodb(selected_index)
        
        candidates = find_candidates_for_text(reconstructed_index, input_text)
        results = [(i, data_dict[i]) for i in candidates]

        return render_template('results.html', results=results, input_text=input_text, selected_index=selected_index)
    
    return render_template('index.html', filtered_collections=filtered_collections)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)














# # Flask route for the web interface
# @app.route('/', methods=['GET', 'POST'])
# def index():
#     if request.method == 'POST':
#         input_text = request.form.get('input_text')
#         candidates = find_candidates_for_text(reconstructed_index, input_text)
#         results = [(i, data_dict[i]) for i in candidates]
#         return render_template('results.html', results=results, input_text=input_text)
#     return render_template('index.html')

# # Run the load_data function at startup to load data and index into memory
# load_data()

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)
