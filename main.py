from typing import LiteralString
import numpy as np
import sys
import mmap
import time


def find_sep(mm, start:int =0, sep: bytes=b';'):
    while (start < len(mm)):
        if mm[start] == sep:
            return start
        else:
            start += 1
    return -1

def main():
    start = time.time()
    file_name: LiteralString = f"measurements.txt"
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    with open(file_name, "r", encoding="utf-8") as f:
        mm: np.memmap = np.memmap(file_name, mode="r", dtype='S1')
        print(mm[:find_sep(mm, sep=b'\n')])
        calculations = {}
        calculations[mm[:find_sep(mm)].tobytes().decode()] = np.array([float(mm[find_sep(mm)+1:find_sep(mm, sep=b'\r')].tobytes()), 0.0, 0.0])
        print(calculations)

    end = time.time()
    print(f"{end-start}s Total execution time")
    

if __name__ == "__main__":
    main()