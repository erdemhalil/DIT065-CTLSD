#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from pyspark import SparkContext
import numpy as np


def generate_hash_tables(seed: int) -> list:
    """
    Generate tabulation tables using the seed
    """
    np.random.seed(seed)
    tables = []
    for _ in range(4):  # We have 4 sets of 16-bit tables
        table = np.random.randint(0, 2**32, size=2**16, dtype=np.uint32)
        tables.append(table)
    return tables


def tabulation_hash(x: np.uint64, tables: list) -> np.uint32:
    """
    Hash a 64-bit integer key x into 32-bit hash value
    """
    x = int(x)
    result = np.uint32(0)
    for i in range(4):  # We have 4 sets of 16-bit tables
        part = np.uint32((x >> (i * 16)) & 0xFFFF)
        result ^= tables[i][part]
    return result


def calculate_rho(value: np.uint32) -> int:
    """
    Position of the leftmost '1' in the binary representation of value
    """
    mask = 1 << 32  # Mask for the leftmost bit
    position = 0    # Position of the leftmost bit, initially 0

    while mask:
        if value & mask:    # If the bit is set, return position
            return position
        mask >>= 1       # Shift the mask to the right by 1
        position += 1    # Increment the position by 1
    return position


def estimate_cardinality(M: np.ndarray) -> float:
    """
    Return the present cardinality estimate
    """
    two_pow_32 = 2**32  # 2^32 for efficiency
    m = len(M)
    alpha_m = 0.7213 / (1 + 1.079 / m)  # Bias correction factor
    Z = 1.0 / np.sum(np.power(2.0, -np.float64(M)))   # Harmonic mean of the registers
    E = alpha_m * np.power(m, 2) * Z    # Raw estimate
    V = np.count_nonzero(M == 0)  # Number of empty registers
    if E <= 5/2 * m and V > 0:
        return m * np.log(m / V)
    elif E > 1/30 * two_pow_32:
        return -two_pow_32 * np.log(1 - (E / two_pow_32))
    return E

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = \
                                    'Compute Twitter followers.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    start = time.time()
    sc = SparkContext(master = f'local[{args.num_workers}]')

    lines = sc.textFile(args.filename)

    # HyperLogLog parameters
    m = 2**10
    seed = 42

    # Generate hash tables and broadcast them
    tables = generate_hash_tables(seed=seed)
    tables_bc = sc.broadcast(tables)

    # Read user IDs from the input file and convert them to 64-bit unsigned integers
    user_ids = lines.flatMap(lambda line: line.split()).map(lambda x: np.uint64(int(x)))

    def map_to_hyperloglog(user_id: np.uint64):
        """
        Map user IDs to indexes and rho values
        """
        j = (user_id * np.uint64(0xc863b1e7d63f37a3)) >> np.uint64(64 - np.log2(m))
        hash = tabulation_hash(user_id, tables_bc.value)
        rho = calculate_rho(hash)
        return j, rho
    
    # Map user IDs to indexes and rho values
    mapped_rdd = user_ids.map(map_to_hyperloglog)
    # Reduce by key (index) using the maximum rho value
    reduced_rdd = mapped_rdd.reduceByKey(max)

    M = np.zeros(m, dtype=np.uint8)
    # Collect the results and update the maximum rho values
    for j, rho in reduced_rdd.collect():
        M[j] = max(M[j], rho)

    estimated_num_users = int(estimate_cardinality(M))
    
    end = time.time()
    
    total_time = end - start

    print(f'extimated number of users: {estimated_num_users}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time}')