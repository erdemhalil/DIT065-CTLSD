from __future__ import annotations
import numpy as np
import numpy.typing as npt
from typing import Optional, Callable
from tabulation_hash import TabulationHash

class HyperLogLog:
    _h: TabulationHash
    _f: Callable[[np.uint64],np.uint64]
    _M: npt.NDArray[np.uint8]
    
    def __init__(self, m: int, seed: Optional[int] = None):
        """
        Initialize a HyperLogLog sketch
        """
        if m & (m - 1) != 0 or m <= 0:
            raise ValueError("m must be a power of 2 and positive")
        self._M = np.zeros(m, dtype=np.uint8)
        self._h = TabulationHash(seed)
        self._f = lambda x: (x * np.uint64(0xc863b1e7d63f37a3)) >> np.uint64(64 - np.log2(m))

    def __call__(self, x: np.uint64):
        """
        Add element into the sketch
        """
        x = np.uint64(x)    # Ensure x is a 64-bit unsigned integer
        j = self._f(x)      # Index for the register
        hash = self._h(x)   # Hash value of
        rho = self._rho(hash)
        self._M[j] = max(self._M[j], rho)

    def estimate(self)->float:
        """
        Return the present cardinality estimate
        """
        two_pow_32 = 2**32  # 2^32 for efficiency
        m = len(self._M)
        alpha_m = 0.7213 / (1 + 1.079 / m)  # Bias correction factor
        Z = 1.0 / np.sum(np.power(2.0, -np.float64(self._M)))   # Harmonic mean of the registers
        E = alpha_m * np.power(m, 2) * Z    # Raw estimate
        V = np.count_nonzero(self._M == 0)  # Number of empty registers
        if E <= 5/2 * m and V > 0:
            return m * np.log(m / V)
        elif E > 1/30 * two_pow_32:
            return -two_pow_32 * np.log(1 - (E / two_pow_32))
        return E
    
    def merge(self, other: HyperLogLog) -> HyperLogLog:
        """
        Merge two sketches
        """
        if len(self._M) != len(other._M):
            raise ValueError("Sketches must have the same register size")
        self._M = np.maximum(self._M, other._M)
        return self

    def _rho(self, value: np.uint64) -> int:
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
    
if __name__ == "__main__":
    hll = HyperLogLog(m=1024, seed=42)
    for i in range(1, 1000000):
        hll(i)
    print("Estimated cardinality: ", hll.estimate())