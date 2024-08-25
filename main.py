from typing import LiteralString
import numpy as np
import sys
import mmap
import time
from multiprocessing import Process, Lock, Manager



def process(split, l, calculations):
    #print(len(split))
    indx = 0
    split_map = {}
    while indx < len(split):
        semi = find_sep(split, indx)
        city_name = split[indx:semi].tobytes().decode()
        crlf = find_sep(split, semi+1, b'\r')
        value = float(split[semi+1:crlf].tobytes())
        indx = crlf + 2
        if city_name not in split_map:
            split_map[city_name] = np.array(value)
        else:
            split_map[city_name] = np.append(split_map[city_name], value)
    l.acquire()
    #print(f"built internal map {split_map}")
    for key, val in split_map.items():
        if key in calculations:
            current = calculations[key]
            calculations[key][0] = min(current[0], np.min(val))
            calculations[key][1] += np.sum(val)
            calculations[key][2] = max(current[2], np.max(val))
            calculations[key][3] += float(val.size)
            #print(f"updating key {calculations[key]}")
        else:    
            calculations[key] = np.array([np.min(val), np.sum(val), np.max(val), float(val.size)])
            #print(f"adding key {calculations[key]}")
    l.release()

def find_sep(mm, start:int =0, sep: bytes=b';'):
    while (start < len(mm)):
        if mm[start] == sep:
            return start
        else:
            start += 1
    return len(mm)

def main():
    start_time = time.time()
    file_name: LiteralString = f"measurements.txt"
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    with open(file_name, "r", encoding="utf-8") as f:
        mm: np.memmap = np.memmap(file_name, mode="r", dtype='S1')
        lock = Lock()
        NUM_PROCESS = 15
        processes = []
        start = 0
        data_split = len(mm)//NUM_PROCESS
        calculations = Manager().dict()
        for i in range(NUM_PROCESS):
            start_end = (i + 1) * data_split
            end = find_sep(mm, start_end, b'\n')
            #print(f"adding process with {start},{end}")
            processes.append(Process(target=process, args=(mm[start:end],lock, calculations)))
            processes[i].start()
            start = end + 1
            if (start >= len(mm)):
                break
        for p in processes:
            p.join()
            

        sys.stdout.write("{")
        for key, val in calculations.items():
            sys.stdout.write(f"{key}={val[0]:.2f}/{val[1]/val[3]:.2f}/{val[2]:.2f},")
        sys.stdout.write("}\n")

    end_time = time.time()
    sys.stdout.write(f"{end_time-start_time}s Total execution time")
    sys.stdout.flush()
    

if __name__ == "__main__":
    main()