# import hashlib
# import re
# from collections import defaultdict
# from itertools import combinations
# from utils.utils import clean_document, shingle, minhash
# from joblib import Parallel, delayed
# import pandas as pd

# class LSH:
#     """Locality Sensitive Hashing (LSH) using MinHash and Banding for approximate near-duplicate detection.
    
#     This class implements the LSH technique to efficiently find near-duplicate documents by hashing
#     similar documents into the same buckets through MinHashing and banding techniques. It consists of
#     the following key steps:
    
#     - MinHash signatures computation: Shingle the documents and compute their MinHash signatures.
#     - Banding: Split the MinHash signatures into bands and hash the bands to identify candidate pairs.
#     - Duplicate detection: Detect exact duplicates and track them.
#     """

#     def __init__(self, num_hashes=100, num_bands=20, rows_per_band=5, k=5):
#         """
#         Initializes the LSH instance with the specified number of hash functions, bands, and shingle size.
        
#         Raises:
#             AssertionError: If the number of hash functions is not equal to num_bands * rows_per_band.
#         """
#         self.num_hashes = num_hashes
#         """num_hashes (int): Total number of hash functions used for MinHash signature computation."""
#         self.num_bands = num_bands
#         """num_bands (int): Number of bands used for LSH banding."""
#         self.rows_per_band = rows_per_band
#         """rows_per_band (int): Number of rows in each band (bands * rows_per_band must equal num_hashes)."""
#         self.index = defaultdict(list)
#         """index (defaultdict): A dictionary used to store bands and document IDs for candidate identification."""
#         self.unique_docs = {}
#         """unique_docs (dict): A dictionary of unique documents (after removing exact duplicates)."""
#         self.cleaned_docs = {}
#         """cleaned_docs (dict): A dictionary of cleaned documents after pre-processing."""
#         self.candidate_pairs = set()
#         """candidate_pairs (set): A set of candidate document pairs found using LSH."""
#         self.exact_duplicates = {}
#         """exact_duplicates (dict): A dictionary of exact duplicates, mapping original doc IDs to duplicate doc IDs."""
#         self.k = k
#         """k (int): The shingle size (number of words or characters in each shingle)."""
    
#     def remove_duplicates(self, docs):
#         """Removes exact duplicates from the provided documents and stores the unique ones.
        
#         This method scans through the provided documents, identifying and removing exact duplicates.
#         It stores the first occurrence of each document in the `unique_docs` attribute and tracks duplicates
#         in the `exact_duplicates` attribute.
        
#         Args:
#             docs (dict): A dictionary where keys are document IDs and values are document contents.
        
#         Side Effects:
#             - Populates the `unique_docs` dictionary with unique documents.
#             - Populates the `exact_duplicates` dictionary with mappings from original document IDs to their duplicates.
#         """
#         seen_docs = {}
#         for doc_id, doc in docs.items():
#             if doc not in seen_docs:
#                 self.unique_docs[doc_id] = doc
#                 seen_docs[doc] = doc_id  # Track first occurrence of the document
#             else:
#                 # Track exact duplicates
#                 original_id = seen_docs[doc]
#                 if original_id not in self.exact_duplicates:
#                     self.exact_duplicates[original_id] = []
#                 self.exact_duplicates[original_id].append(doc_id)

#     def compute_minhash_signatures(self, docs):
#         """Computes MinHash signatures for each unique document using parallel processing.
        
#         This method performs the following steps:
#         1. Removes exact duplicates from the documents using `remove_duplicates`.
#         2. Cleans each document (e.g., tokenization, removing punctuation).
#         3. Generates shingles for each document based on the specified shingle size (`k`).
#         4. Computes MinHash signatures for each document using the `minhash` function.
        
#         Args:
#             docs (dict): A dictionary where keys are document IDs and values are document contents.
        
#         Returns:
#             dict: A dictionary where keys are document IDs and values are the MinHash signatures (lists of hash values).
        
#         Side Effects:
#             - Populates the `cleaned_docs` dictionary with cleaned versions of the documents.
#             - Populates the `shingle_sets` dictionary with shingles for each document.
#             - Populates the `signatures` dictionary with MinHash signatures for each document.
#         """
#         # self.remove_duplicates(docs)
#         self.cleaned_docs = {doc_id: clean_document(doc) for doc_id, doc in docs.items()}
#         # self.shingle_sets = {doc_id: shingle(doc, self.k) for doc_id, doc in self.cleaned_docs.items()}
#         self.shingle_sets = {doc_id: shingle(doc, self.k) for doc_id, doc in self.cleaned_docs.items()}
        
#         # Parallel computation of MinHash signatures
#         signatures = Parallel(n_jobs=-1)(delayed(minhash)(shingles, self.num_hashes) for doc_id, shingles in self.shingle_sets.items())
#         self.signatures = dict(zip(self.shingle_sets.keys(), signatures))
        
#         doc_ids = list(self.shingle_sets.keys())
#         self.signature_df = pd.DataFrame(signatures,index=doc_ids).T
#         return self.signature_df

#     def banding(self):
#         """Splits the signature DataFrame into bands and hashes each band to find candidate pairs."""
#         self.candidate_pairs = set()
        
#         # Iterate over bands
#         for band_idx in range(self.num_bands):
#             start = band_idx * self.rows_per_band
#             end = start + self.rows_per_band

#             # Extract the band from the DataFrame
#             band_df = self.signature_df.iloc[start:end, :]
            
#             # Hash each column in the band and store in index
#             for doc_id, band_values in band_df.items():  # Use .items() for DataFrame columns
#                 # Generate a unique hash for the band
#                 band_hash = hashlib.md5(band_values.values.tobytes()).hexdigest()
#                 self.index[(band_idx, band_hash)].append(doc_id)

#         # Find candidate pairs
#         for doc_ids in self.index.values():
#             if len(doc_ids) > 1:
#                 for i in range(len(doc_ids)):
#                     for j in range(i + 1, len(doc_ids)):
#                         self.candidate_pairs.add((doc_ids[i], doc_ids[j]))

#         return self.candidate_pairs
    
#     def get_minhash_signature(self, text, num_hashes=100):
#         """Generate a MinHash signature for the input text."""
#         cleaned_text = clean_document(text)  # Assuming `clean_document` function exists
#         shingles = shingle(cleaned_text, k=self.k)  # Assuming `shingle` function exists
#         return minhash(shingles, num_hashes)  # Assuming `minhash` function exists

#     def find_candidates_for_text(self, text):
#         """Find candidate pairs for an input text using banding."""
#         # Compute the MinHash signature for the input text
#         text_signature = self.get_minhash_signature(text)
        
#         # Convert signature into a DataFrame for banding (like the existing signature_df)
#         text_signature_df = pd.DataFrame(text_signature).T  # Transpose to make rows as hash functions
        
#         candidate_docs = set()
        
#         # Apply banding on the input text
#         for band_idx in range(self.num_bands):
#             start = band_idx * self.rows_per_band
#             end = start + self.rows_per_band

#             # Extract the band for the input text
#             band_values = text_signature_df.iloc[0, start:end].values  # Get band for input text
#             band_hash = hashlib.md5(band_values.tobytes()).hexdigest()
            
#             # Check if any documents share this band hash
#             if (band_idx, band_hash) in self.index:
#                 candidate_docs.update(self.index[(band_idx, band_hash)])

#         return candidate_docs



import hashlib
import re
from collections import defaultdict
from itertools import combinations
from utils.utils import clean_document, shingle, minhash
from joblib import Parallel, delayed
import numpy as np

class LSH:
    """Locality Sensitive Hashing (LSH) using MinHash and Banding for approximate near-duplicate detection."""

    def __init__(self, num_hashes=100, num_bands=20, rows_per_band=5, k=5, batch_size=5000):
        """Initialize LSH with specified parameters and a batch size to process large data in chunks."""
        assert num_bands * rows_per_band == num_hashes, "num_hashes must be equal to num_bands * rows_per_band"
        self.num_hashes = num_hashes
        self.num_bands = num_bands
        self.rows_per_band = rows_per_band
        self.k = k
        self.batch_size = batch_size
        self.index = defaultdict(list)  # Band-indexed dictionary for candidate identification
        self.signatures = {}  # To store MinHash signatures for each document
        self.cleaned_docs = {}
        self.candidate_pairs = set()

    # def remove_duplicates(self, docs):
    #     """Remove exact duplicates from the documents."""
    #     unique_docs = {}
    #     duplicates = defaultdict(list)
    #     for doc_id, doc in docs.items():
    #         if doc not in unique_docs:
    #             unique_docs[doc_id] = doc
    #         else:
    #             duplicates[unique_docs[doc]] = doc_id
    #     self.cleaned_docs = unique_docs
    #     self.exact_duplicates = duplicates

    def compute_minhash_signatures(self, docs):
        """Compute MinHash signatures in batches for efficiency and reduced memory usage."""
        # self.remove_duplicates(docs)  # Remove duplicates to minimize processing
        self.cleaned_docs = {doc_id: clean_document(doc) for doc_id, doc in docs.items()}
        # self.shingle_sets = {doc_id: shingle(doc, self.k) for doc_id, doc in self.cleaned_docs.items()}
        # self.shingle_sets = {doc_id: shingle(doc, self.k) for doc_id, doc in self.cleaned_docs.items()}
        doc_ids = list(self.cleaned_docs.keys())
        
        for start in range(0, len(doc_ids), self.batch_size):
            batch_ids = doc_ids[start:start + self.batch_size]
            batch_shingles = {doc_id: shingle(self.cleaned_docs[doc_id], self.k) for doc_id in batch_ids}
            batch_signatures = Parallel(n_jobs=-1)(delayed(minhash)(shingles, self.num_hashes) for doc_id, shingles in batch_shingles.items())
            
            # Store each signature as a list in the dictionary
            for doc_id, signature in zip(batch_ids, batch_signatures):
                self.signatures[doc_id] = signature
            
            print(f"Processed batch: {start} to {start + len(batch_ids)}")

        return self.signatures

    def banding(self):
        """Apply LSH banding to find candidate pairs."""
        self.candidate_pairs.clear()  # Reset candidate pairs for fresh processing

        # Process each signature and apply banding
        for doc_id, signature in self.signatures.items():
            for band_idx in range(self.num_bands):
                start = band_idx * self.rows_per_band
                band = tuple(signature[start:start + self.rows_per_band])  # Convert band into a tuple for hashing
                band_hash = hashlib.md5(str(band).encode()).hexdigest()  # Hash the band

                # Store doc_id in the index for this band
                self.index[(band_idx, band_hash)].append(doc_id)

        # Identify candidate pairs by looking for documents that share bands
        for doc_ids in self.index.values():
            if len(doc_ids) > 1:
                for i in range(len(doc_ids)):
                    for j in range(i + 1, len(doc_ids)):
                        self.candidate_pairs.add((doc_ids[i], doc_ids[j]))

        return self.candidate_pairs

    def get_minhash_signature(self, text):
        """Generate a MinHash signature for a single input text."""
        cleaned_text = clean_document(text)
        shingles = shingle(cleaned_text, self.k)
        return minhash(shingles, self.num_hashes)

    def find_candidates_for_text(self, text):
        """Find candidate pairs for an input text by computing its MinHash signature and applying banding."""
        text_signature = self.get_minhash_signature(text)
        candidate_docs = set()

        # Apply banding on the text signature
        for band_idx in range(self.num_bands):
            start = band_idx * self.rows_per_band
            band = tuple(text_signature[start:start + self.rows_per_band])
            band_hash = hashlib.md5(str(band).encode()).hexdigest()

            # Check if any documents share this band hash
            if (band_idx, band_hash) in self.index:
                candidate_docs.update(self.index[(band_idx, band_hash)])

        return candidate_docs