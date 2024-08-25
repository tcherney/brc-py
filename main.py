from typing import LiteralString
import sys
import os
import mmap
import time
import multiprocessing

def process(file_name, start, end):
    offset = (start // mmap.ALLOCATIONGRANULARITY) * mmap.ALLOCATIONGRANULARITY
    split_map = {}
    with open(file_name, "rb") as f:
        with mmap.mmap(f.fileno(), end-offset, access=mmap.ACCESS_READ, offset=offset) as mm:
            mm.seek(start-offset)
            for line in iter(mm.readline, b""):
                sep_indx = line.find(b';')
                city_name = line[:sep_indx]
                value = float(line[sep_indx+1:-2])
                if city_name in split_map:
                    current = split_map[city_name]
                    current[0] = min(current[0], value)
                    current[1] += value
                    current[2] = max(current[2], value)
                    current[3] += 1
                else:
                    split_map[city_name] = [value, value, value, 1]
    #print("finished calc")
    #print(split_map)
    return split_map

def main():
    start_time = time.time()
    file_name: LiteralString = f"measurements.txt"
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    length = os.path.getsize(file_name)
    with open(file_name, "rb") as f:
        mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
        NUM_PROCESS = os.cpu_count() - 1
        splits = []
        start = 0
        data_split = length//NUM_PROCESS
        for i in range(NUM_PROCESS):
            start_end = (i + 1) * data_split
            end = mm.find(b'\n', start_end)
            end = end + 1 if end != -1 else length
            # objects too big for pickling to pass around have to work with file from each process
            splits.append((file_name, start, end))
            start = end + 1
            if (start >= length):
                break
        mm.close()
        with multiprocessing.Pool(processes=NUM_PROCESS) as pool:
            ret = pool.starmap(process, splits)
        
        calculations = {}
        for split_map in ret:
            for key, val in split_map.items():
                if key in calculations:
                    current = calculations[key]
                    calculations[key][0] = min(current[0], val[0])
                    calculations[key][1] += val[1]
                    calculations[key][2] = max(current[2], val[2])
                    calculations[key][3] += val[3]
                    #print(f"updating key {calculations[key]}")
                else:    
                    calculations[key] = val
                    #print(f"adding key {calculations[key]}")
        sys.stdout.write("{")
        for key, val in calculations.items():
            sys.stdout.write(f"{key.decode()}={val[0]:.2f}/{val[1]/val[3]:.2f}/{val[2]:.2f},")
        sys.stdout.write("}\n")

    end_time = time.time()
    sys.stdout.write(f"{end_time-start_time}s Total execution time")
    sys.stdout.flush()
    

if __name__ == "__main__":
    main()