#!/usr/bin/env python3

import numpy as np
import numpy.typing as npt
import pandas as pd
import csv
import argparse
import time
from operator import itemgetter
from typing import Dict, Tuple, List, Optional, Set

def load_glove(filename: str)->Tuple[Dict[str,int],Dict[int,str],
                                        npt.NDArray[np.float64]]:
    """
    Loads the glove dataset. Returns three things:
    A dictionary that contains a map from words to rows in the dataset.
    A reverse dictionary that maps rows to words.
    The embeddings dataset as a NumPy array.
    """
    df = pd.read_table(filename, sep=' ', index_col=0, header=None,
                           quoting=csv.QUOTE_NONE)
    word_to_idx: Dict[str,int] = dict()
    idx_to_word: Dict[int,str] = dict()
    for (i,word) in enumerate(df.index):
        word_to_idx[word] = i
        idx_to_word[i] = word
    return (word_to_idx, idx_to_word, df.to_numpy())

def normalize(X: npt.NDArray[np.float64])->npt.NDArray[np.float64]:
    """
    Reads an n*d matrix and normalizes all rows to have unit-length (L2 norm)
    
    Implement this function using array operations! No loops allowed.
    """
    norms = np.linalg.norm(X, axis=1, keepdims=True)
    return X / norms

def construct_queries(queries_fn: str, word_to_idx: Dict[str,int],
                          X: npt.NDArray[np.float64]) -> \
                          Tuple[npt.NDArray[np.float64],List[str]]:
    """
    Reads queries (one string per line) and returns:
    - The query vectors as a matrix Q (one query per row)
    - Query labels as a list of strings
    """
    with open(queries_fn, 'r') as f:
        queries = f.read().splitlines()
    Q = np.zeros((len(queries), X.shape[1]))
    for i in range(len(queries)):
        Q[i,:] = X[word_to_idx[queries[i]],:]
    return (Q,queries)

class RandomHyperplanes:
    """
    This class mimics the interface of sklearn:
    - the constructor sets the number of hyperplanes
    - the random hyperplanes are drawn when fit() is called 
      (input dimension is set)
    - transform actually transforms the vectors
    - fit_transform does fit first, followed by transform
    """
    def __init__(self, d: int, seed: Optional[int] = None)->None:
        """
        Sets the number of hyperplanes (d) and the optional random number seed
        """
        self._d = d
        self._seed = seed
        self.R = None

    def fit(self, X: npt.NDArray[np.float64])->None:
        """
        Draws _d random hyperplanes, that is, by drawing _d Gaussian unit 
        vectors of length determined by the second dimension (number of 
        columns) of X
        """
        rng = np.random.default_rng(self._seed)
        d = X.shape[1]
        self.R = rng.standard_normal((self._d, d))
        self.R /= np.linalg.norm(self.R, axis=1, keepdims=True)

    def transform(self, X: npt.NDArray[np.float64])->npt.NDArray[np.uint8]:
        """
        Project the rows of X into binary vectors
        """
        X_prime = np.dot(X, self.R.T)
        X_binary = (X_prime > 0).astype(np.uint8)
        return X_binary

    def fit_transform(self, X: npt.NDArray[np.float64])->npt.NDArray[np.uint8]:
        """
        Calls fit() followed by transform()
        """
        self.fit(X)
        return self.transform(X)

class LocalitySensitiveHashing:
    """
    Performs locality-sensitive hashing by projecting unit vectors to binary vectors
    """

    # type hints for intended members
    _d: int # number of random hyperplanes
    _k: int # hash function length
    _L: int # number of hash functions (tables)
    _hash_functions: npt.NDArray[np.int64] # the actual hash functions
    _random_hyperplanes: RandomHyperplanes # random hyperplanes object
    _H: List[Dict[Tuple[np.uint8],Set[int]]] # hash tables
    _X: npt.NDArray[np.float64] # the original data
    
    def __init__(self, d: int, k: int, L: int, seed: Optional[int]):
        """
        Sets the parameters
        - d internal dimensionality (used with random hyperplanes)
        - k length of hash functions (how many elementary hash functions 
          to concatenate)
        - L number of hash tables
        - seed random number generator seed (used for intializing random 
          hyperplanes; also used to seed the random number generator
          for drawing the hash functions)
        """
        self._d = d
        self._k = k
        self._L = L
        rng = np.random.default_rng(seed)
        # draw the hash functions here
        # (essentially, draw a random matrix of shape L*k with values in
        # 0,1,...,d-1)
        self._hash_functions = rng.integers(low=0, high=d, size=(L, k))
        # also initialize the random hyperplanes
        self._random_hyperplanes = RandomHyperplanes(d, seed)
        self._H = [{} for _ in range(L)]

    def fit(self, X: npt.NDArray[np.float64])->None:
        """
        Fit random hyperplanes
        Then project the dataset into binary vectors
        Then hash the dataset L times into the L hash tables
        """
        self._X = X
        
        # Project the dataset into binary vectors
        X_binary = self._random_hyperplanes.fit_transform(X)

        # Hash the dataset L times into the L hash tables
        for l in range(self._L):
            for i, x in enumerate(X_binary):
                h = tuple(x[self._hash_functions[l]])
                if h not in self._H[l]:
                    self._H[l][h] = set()
                self._H[l][h].add(i)


    def query(self, q: npt.NDArray[np.float64])->npt.NDArray[np.int64]:
        """
        Queries one vector
        Returns the *indices* of the nearest neighbors in descending order
        That is, if the returned array is I, then X[I[0]] is the nearest 
        neighbor (if the vector was member of the dataset, then typically 
        this would be itself), X[I[1]] the second nearest etc.
        """
        

        # Project the query into a binary vector
        q_binary = self._random_hyperplanes.transform(q[np.newaxis, :])[0]
        # Then hash it L times
        # Collect all indices from the hash buckets
        candidates = set()
        for l in range(self._L):
            h = tuple(q_binary[self._hash_functions[l]])
            if h in self._H[l]:
                candidates.update(self._H[l][h])
        # Then compute the dot products with those vectors
        dot_products = np.dot(self._X[list(candidates)], q)
        # Finally sort results in *descending* order and return the indices
        sorted_indices = np.argsort(-dot_products)  # descending order
        # Project the query into a binary vector
        return np.array(list(candidates))[sorted_indices]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', help='Random hyperplanes dimension', type=int,
                            required = True)
    parser.add_argument('-k', help='Hash function length', type=int,
                            required = True)
    parser.add_argument('-L', help='Number of hash tables (functions)', type=int,
                            required = True)
    parser.add_argument('dataset', help='Glove dataset filename',
                            type=str)
    parser.add_argument('queries', help='Queries filename', type=str)
    args = parser.parse_args()
    
    (word_to_idx, idx_to_word, X) = load_glove(args.dataset)


    X = normalize(X)

    (Q,queries) = construct_queries(args.queries, word_to_idx, X)

    t1 = time.time()
    lsh = LocalitySensitiveHashing(args.d, args.k, args.L, 1234)
    
    t2 = time.time()    
    lsh.fit(X)
    
    t3 = time.time()
    neighbors = list()
    for i in range(Q.shape[0]):
        q = Q[i,:]
        I = lsh.query(q)
        neighbors.append([idx_to_word[i] for i in I][1:4])
    t4 = time.time()

    print('init took',t2-t1)
    print('fit took', t3-t2)
    print('query took', t4-t3)
    print('total',t4-t1)

    for i in range(Q.shape[0]):
        print(f'{queries[i]}: {" ".join(neighbors[i])}') 

