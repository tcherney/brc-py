import time
import polars as pl
import sys


def main():
    start = time.time()
    file_name = f"measurements.txt"
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    pl.Config.set_streaming_chunk_size(8_000_000)
    q = pl.scan_csv(file_name, separator=";", schema={"City": pl.String, "Measurement": pl.Float64}, has_header=False).group_by("City").agg([pl.min("Measurement").alias("min"),pl.mean("Measurement").alias("mean"),  pl.max("Measurement").alias("max")]).sort("City").collect(streaming=True)
    print("{",", ".join(f"{data[0]}={data[1]:.2f}/{data[2]:.2f}/{data[3]:.2f}" for data in q.iter_rows()), "}", sep="")
    end = time.time()
    print(f"{end-start}s Total execution time")
    

if __name__ == "__main__":
    main()