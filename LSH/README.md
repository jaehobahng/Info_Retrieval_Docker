# ✨ Deduplication ✨

Our python package `deduplication` uses a multitude of methods to solve the NLP problem of near-deduplication of text. The following is a brief description of the algorithms along with instructions on how to use the package. 

## Features 

### Base LSH

#### Key Variables
- **`num_hashes`**: Number of hash functions, determining the MinHash signature length and similarity accuracy.
- **`num_bands`**: Number of bands to divide the signature, affecting sensitivity and selectivity.
- **`rows_per_band`**: Rows in each band; impacts selectivity and must satisfy `num_bands * rows_per_band = num_hashes`.
- **`k`**: Shingle size (number of words per shingle) that sets the granularity of document segmentation.

#### Algorithm Steps
1. **Remove unique documents**: Store in separate dictionary
2. **Shingling**: Break each document into overlapping word sequences (k-shingles) to capture local structure.
3. **MinHash Signatures**: Apply multiple hash functions to each document’s shingles, keeping only the minimum hash per function to create a signature matrix where columns are documents and rows are hash functions.
4. **Banding**: Split each signature into band(groups of rows) ➔ Within each band, the sequence of hash values is grouped and treated as a single entity ➔ If two documents have the same band hash they are considered to be in the same "bucket" for that band.
5. **Candidate Pairs**: Identify documents in the same bucket as candidate pairs.
6. **Clustering**: Form clusters of near-duplicate documents based on shared buckets, representing high similarity.

### LSH Forest
The `LSH Forest` class under `src/deduplication` extends the base LSH class by creating multiple trees in the forest. Each tree corresponds to an independent LSH producing independent candidate pairs. The final candidate pairs are merged via majority voting. This allows more robustness and less false positive rate by favoring candidate pairs that appear consistently.

#### Key Variables
- **`num_trees`**: Number of trees, creating number of LSH trees

### LSH Multi Probe Approach

The `LSHImproved` class under `src/deduplication` uses a multi-probe locality sensitive hashing algorithm for efficient near-duplicate detection and similarity search for large document collections. While the traditional LSH implementation, which we discussed earlier, creates fixed buckets for the hash tokens, the multi-probe approach perturbates the hash tokens in order to find “nearby” buckets for each document. This allows for an easier way to find similar documents while reducing the overall storage needed in this algorithm. 

Specifically, we utilized MinHash signatures to represent each document and tested which perturbation methods (`bit_flip`, `nearby_banding`, and `gaussian`) along with an ideal probe number provided the optimal number of clusters and was most efficient time-wise. Thus, this improved version of LSH allows us to identify candidate pairs that share one or more similar bands with more efficiency than the traditional method. 

#### Key Variables 
`num_probes`: the number of probes needed for multi-probe search
`banding_method`: this indicates the hashing perturbation method  (bit_flip, nearby_banding, and gaussian) 

### Bloom Filter 

The `BloomFilter` class under `src/deduplication` is an implementation of a probabilistic data structure used for efficient membership testing, designed to determine whether an element might be in a set or is definitely not in it. This implementation uses a configurable number of hash functions and a bit array to track elements, allowing for a specified false positive rate. 

The class accepts two main parameters: `n`, the maximum expected number of elements, and `f`, the desired false positive rate. When an item is added, the `add` method tokenizes the item and generates n-grams (substrings of length 1 to 3), then hashes these n-grams across multiple hash functions, setting the respective bits in the bit array. The `query` method performs a similar hashing process to check if all related bits are set, indicating that the item might be present. This Bloom filter is highly space-efficient and well-suited for applications where some false positives are acceptable, but false negatives are not.

The `BloomFilter_QF` class combines a Bloom filter with a quotient filter for enhanced space efficiency. It splits each hash into a **quotient** (bucket index) and **remainder** (stored in the bucket). This structure allows efficient membership testing and lower false positives by using the remainder to confirm matches within each bucket.


## Requirements
**These will be the technical requirements to run the code**
- python = "^3.11"
- matplotlib = "^3.9.2"
- myst-nb = "^1.1.2"
- mmh3 = "^5.0.1"
- bitarray = "^3.0.0"
- nltk = "^3.9.1"
- xxhash = "^3.5.0"


## Installation

**We describe the bash comments to run each file**

```bash
pip install deduplication
```

## Usage

Explore [Usage Explanation Notebook](./docs/example.ipynb) for a more detailed explanation of the package

### How to run package in script or notebook
#### Running base LSH

```{python}
tsv_dict = read_tsv('../data/onek.tsv')

num_hashes = 100
num_bands = 20
rows_per_band = 5
k = 10

lsh = LSH(num_hashes=num_hashes, num_bands=num_bands, rows_per_band=rows_per_band, k=k)
signatures = lsh.compute_minhash_signatures(tsv_dict)
clusters = collection_deduplication(lsh)

for key, value in list(clusters.items())[:20]:
    print(key, value)
```


### How to run package from a terminal

Arguments:
- -d, --indir (str): Required. Directory path of the input file.
- -t, --case (str): Required. Type of use case.
- -s, --save (str): Optional. Whether to output results to a text file ('y' or 'n').
- -e, --example (str): Optional. Document to query.
- -n, --numhash (int): Optional. Number of hash functions to use.
- -b, --numband (int): Optional. Number of bands.
- -r, --row (int): Optional. Number of rows per band.
- -k, --shinlen (int): Optional. Length of shingles.
- -c, --treesize (int): Optional. Size of the tree.
- -m, --method (str): Optional. Default is 'LSH'. Specifies the method to use. Options: 'baseline', 'LSH', 'LSH_mp', 'LSH_forest'.

Example Terminal Code:
- python -m deduplication -d './data/onek.tsv' -t 'deduplication' -s 'y'
- python -m deduplication -d './data/threehundred.tsv' -t 'deduplication'
- python -m deduplication -d './data/threehundred.tsv' -t 'deduplication' -m 'baseline'
- python -m deduplication -d './data/hundred.tsv' -t 'ann' -e 'this is a blank statement'
- python -m deduplication -d './data/onek.tsv' -t 'deduplication' -m "LSH_forest"
- python -m deduplication -d './data/onek.tsv' -t 'deduplication' -m "LSH_forest" -n 200 -b 10 -r 5 -c 4

## Structure

Below is a brief overview of our file structure. We have also added a key to denote additional information about each file

🚫 - Part of .gitignore and not included in the repo 

🛠️ - Part of sphinx or package build, decription of file is not required 

🎨 - Images used in discussion.md, please see this file for further explanation

📚 - Please see sphinx documentation for a detailed description of this file

```
├── 📁 data (🚫)
├── 📁 docs 
│   ├── _build (🛠️)
│   ├── Makefile (🛠️)
│   ├── conf.py (🛠️)
│   ├── example.ipynb → demonstrates an example of usage of this package
│   ├── index.md (🛠️)
│   ├── make.bat (🛠️)
│   └── requirements.txt → required dependencies for this package
├── 📁 images
│   ├── LSH_Band_Row.png (🎨)
│   ├── LSH_Shingle_Length.png (🎨)
│   ├── LSH_number_trees.png (🎨)
│   ├── Viz1.png (🎨)
│   ├── Viz2.png (🎨)
│   ├── bloom_filter_results.png (🎨)
│   ├── hash_functions_FP_rate.png (🎨)
│   ├── lsh_improved_params.png (🎨)
│   └── lsh_s_curve.png (🎨)
├── 📁 notebooks
│   ├── EDA.ipynb → this notebook shows the EDA conducted on our text data
│   ├── exercise.ipynb → solves BloomFilter textbook problems
│   ├── lsh.ipynb → Used to test our implementation of LSH prior to the final version
│   ├── test.ipynb → Used to test our implementation of multi probe LSH prior to the final version
│   ├── visualization_lsh.ipynb → code for LSH graphs 
│   └── visualizations.ipynb → code for LSH graphs 
├── 📁 output → the files within this directory are the desired output for each data file for each near-deduplication algorithm
│   ├── hundredk-LSH_mp.txt
│   ├── hundredk-baseline.txt
│   ├── hundredk-lsh.txt
│   ├── onek-LSH.txt
│   ├── onek-LSH_mp.txt
│   ├── onek-baseline.txt
│   ├── tenk-LSH_mp.txt
│   ├── tenk-baseline.txt
│   ├── tenk-lsh.txt
│   ├── threehundred-LSH_mp.txt
│   ├── threehundred-baseline.txt
│   └── threehundred-lsh.txt
├── 📁 src
│   ├── 📁 deduplication
│   │   ├── LSH.py (📚)
│   │   ├── LSHForest.py (📚)
│   │   ├── LSHImproved.py (📚)
│   │   ├── __init__.py (📚)
│   │   ├── __main__.py (📚)
│   │   ├── bloom_filter.py (📚)
│   │   ├── dedup.py (📚)
│   └── 📁 utils
│       ├── use_cases.py (📚)
│       ├── utils.py (📚)
│       ├── visualization_lsh.py (📚)
│       └── visualizations.py (📚)
└── 📁 tests
│   ├── test_deduplication.py → test cases for each near-deduplication algorithm
├── discussion.md → describes algorithms in greater detail and answers project questions
├── poetry.lock (🛠️)
├── pyproject.toml (🛠️)
```


## Contributing

Clone and set up the repository with

```bash
git clone TODO && cd deduplication
pip install -e ".[dev]"
```

Install pre-commit hooks with

```bash
pre-commit install
```

Run tests using

```
pytest -v tests
```

