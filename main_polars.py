import time
import polars as pl
import sys


def main():
    start = time.time()
    file_name = f"measurements.txt"
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    q = pl.scan_csv(file_name, separator=";", schema={"City": pl.String, "Measurement": pl.Float64}, has_header=False).group_by("City").agg([pl.min("Measurement").name.suffix("_min"),pl.mean("Measurement").name.suffix("_mean"),  pl.max("Measurement").name.suffix("_max")]).collect(streaming=True)
    print(q)
    end = time.time()
    print(f"{end-start}s Total execution time")
    

if __name__ == "__main__":
    main()