#!/usr/bin/env python3

import numpy as np
from typing import Optional

class TabulationHash:
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the tabulation hash function.
        """
        # raise NotImplementedError()
        self.seed = seed
        self.tables = self._generate_tables()

    def __call__(self, x: np.uint64)->np.uint32:
        """
        Hash a 64-bit integer key x into 32-bit hash value
        """
        
        # The right shift operator (>>) in NumPy does not support 64-bit integers (np.uint64)
        # Using Python's integer type (int)
        x = int(x)
        result = np.uint32(0)
        for i in range(4):  # We have 4 sets of 16-bit tables
            part = np.uint32((x >> (i * 16)) & 0xFFFF)
            result ^= self.tables[i][part]
        return result
    
    def _generate_tables(self):
        """
        Generate tabulation tables using the seed
        """
        if self.seed is not None:
            np.random.seed(self.seed)
        tables = []
        for _ in range(4):  # We have 4 sets of 16-bit tables
            table = np.random.randint(0, 2**16, size=2**16, dtype=np.uint32)
            tables.append(table)
        return tables