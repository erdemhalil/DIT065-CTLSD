import numpy as np
import argparse
import pandas as pd
import csv
import sys
import time
import cupy as cp
from pylibraft.neighbors.brute_force import knn

def linear_scan(X, Q, b = None):
    """
    Perform linear scan for querying nearest neighbor.
    X: n*d dataset
    Q: m*d queries
    b: optional batch size (ignored in this implementation)
    Returns an m-vector of indices I; the value i reports the row in X such 
    that the Euclidean norm of ||X[I[i],:]-Q[i]|| is minimal
    """
    n = X.shape[0]
    m = Q.shape[0]
    I = cp.zeros(m, dtype=cp.int64)
    
    # Transfer data to GPU
    X_gpu = cp.asarray(X)
    Q_gpu = cp.asarray(Q)
    
    if b is None:
        b = m
    
    num_batches = int(cp.ceil(m / b))
    
    for batch_idx in range(num_batches):
        start = batch_idx * b
        end = min((batch_idx + 1) * b, m)
        Q_batch = Q[start:end, :]
        
        # Perform kNN search for the batch
        distances, indices = knn(X_gpu, Q_batch, k=1)

        # Convert indices to a NumPy array before slicing
        indices_np = cp.asnumpy(indices)

        # Convert indices_np[:, 0] back to a CuPy array
        indices_cp = cp.asarray(indices_np[:, 0])

        # Update global indices
        I[start:end] = indices_cp

    
    return I

def load_glove(fn):
    """
    Loads the glove dataset from the file
    Returns (X,L) where X is the dataset vectors and L is the words associated
    with the respective rows.
    """
    df = pd.read_table(fn, sep = ' ', index_col = 0, header = None,
                           quoting = csv.QUOTE_NONE, keep_default_na = False)
    X = cp.asarray(df, dtype = cp.float32)
    L = df.index.tolist()
    return (X, L)

def load_pubs(fn):
    """
    Loads the pubs dataset from the file
    Returns (X,L) where X is the dataset vectors (easting,northing) and 
    L is the list of names of pubs, associated with each row
    """
    df = pd.read_csv(fn)
    L = df['name'].tolist()
    X = cp.asarray(df[['easting','northing']], dtype = cp.float32)
    return (X, L)

def load_queries(fn):
    """
    Loads the m*d array of query vectors from the file
    """
    return np.loadtxt(fn, delimiter = ' ', dtype = np.float32)

def load_query_labels(fn):
    """
    Loads the m-long list of correct query labels from a file
    """
    with open(fn,'r') as f:
        return f.read().splitlines()

if __name__ == '__main__':
    parser = argparse.ArgumentParser( \
          description = 'Perform nearest neighbor queries under the '
          'Euclidean metric using linear scan, measure the time '
          'and optionally verify the correctness of the results')
    parser.add_argument(
        '-d', '--dataset', type=str, required=True,
        help = 'Dataset file (must be pubs or glove)')
    parser.add_argument(
        '-q', '--queries', type=str, required=True,
        help = 'Queries file (must be compatible with the dataset)')
    parser.add_argument(
        '-l', '--labels', type=str, required=False,
        help = 'Optional correct query labels; if provided, the correctness '
        'of returned results is checked')
    parser.add_argument(
        '-b', '--batch-size', type=int, required=False,
        help = 'Size of batches')
    args = parser.parse_args()

    t1 = time.time()
    if 'pubs' in args.dataset:
        (X,L) = load_pubs(args.dataset)
    elif 'glove' in args.dataset:
        (X,L) = load_glove(args.dataset)
    else:
        sys.stderr.write(f'{sys.argv[0]}: error: Only glove/pubs supported\n')
        exit(1)
    t2 = time.time()

    (n,d) = X.shape
    assert len(L) == n

    # Copy queries data to GPU
    t3 = time.time()
    Q = cp.asarray(load_queries(args.queries))
    t4 = time.time()

    assert X.flags['C_CONTIGUOUS']
    assert Q.flags['C_CONTIGUOUS']
    assert X.dtype == np.float32
    assert Q.dtype == np.float32
    
    m = Q.shape[0]
    assert Q.shape[1] == d

    t5 = time.time()
    QL = None
    if args.labels is not None:
        QL = load_query_labels(args.labels)
        assert len(QL) == m
    t6 = time.time()

    # Perform linear scan on GPU
    t7 = time.time()
    I = linear_scan(X, Q, args.batch_size)
    cp.cuda.Stream.null.synchronize()  # Synchronize to ensure computations finish
    t8 = time.time()
    assert I.shape == (m,)

    num_erroneous = 0
    if QL is not None:
        for i in range(len(I)):
            if QL[i] != L[int(I[i])]:
                sys.stderr.write(f'{i}th query was erroneous: got "{L[int(I[i])]}" '
                                    f'but expected "{QL[i]}"\n')
                num_erroneous += 1

    # Copy results back to host
    I_host = cp.asnumpy(I)
    t9 = time.time()
    
    print(f'Loading dataset ({n} vectors of length {d}) took', t2-t1)
    print(f'Performing {m} NN queries took', t8-t7)
    print(f'Number of erroneous queries: {num_erroneous}')
    print(f'Copying {m} queries to GPU took', t4-t3)
    print(f'Copying {m} results back to host took', t9-t8)