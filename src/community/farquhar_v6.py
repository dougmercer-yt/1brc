"""Submitted by Adam Farquhar (Github: adamfarquhar)

Performance
-----------
This implementation is only a fraction of a second slower than doug_booty4, but
approaches the problem of reading the file in a different way.

Doug Discussion
---------------
This submission shows a few cool things.

First, I was really surprised Adam could introduce/reintroduce so many
"creature comforts" into his implementation without affecting performance. I
had removed the global indexing variables because I incorrectly assumed they
would affect performance, but in practice they don't. Similarly, the helpful
CLI doesn't appear to meaningfully affect performance.

Removing the global variables or modifying Adam's revised parse_temp function
didn't impact performance.

Beyond that, it shows a neat strategy for reading the file in chunks using
a `bytearray`. This is a nice alternative to the mmapping approach used
by booty or the line-by-line reading used by infesi.
"""
import argparse
import os
import time
from multiprocessing import Pool

# min, max, sum, count
RMIN = 0
RMAX = 1
RSUM = 2
RCNT = 3

Record = list[int, int, int, int]


def rec_str(loc, rec) -> str:
    return f"{loc.decode('utf-8')}={rec[RMIN]/10:.1f}/{0.1 * rec[RSUM] / rec[RCNT]:.1f}/{rec[RMAX]/10:.1f}"


def parse_temp(buf: bytes, offset: int) -> int:
    # DASH: int = 45  # ord("-")
    # DOT: int = 46  # ord(".")

    if buf[offset] == 45:  # dash ord("-")
        neg = True
        offset += 1
    else:
        neg = False
    if buf[offset + 1] == 46:  # dot ord(".")
        # d.d
        val: int = (buf[offset] - 48) * 10 + (buf[offset + 2] - 48)
    elif buf[offset + 2] == 46:  # dot ord(".")
        # dd.d
        val: int = (buf[offset] - 48) * 100 + (buf[offset + 1] - 48) * 10 + (buf[offset + 3] - 48)
    else:
        raise ValueError(f"Invalid temperature format: {buf} from offset {offset}")
    return val if not neg else -val


def merge_data(target: dict[str, Record], source_data: list[dict[str, Record]]) -> dict[str, Record]:
    """Merge the source data into the target."""
    for result in source_data:
        for key, val in result.items():
            if key in target:
                rec = target[key]
                rec[RMIN] = min(rec[RMIN], val[RMIN])
                rec[RMAX] = max(rec[RMAX], val[RMAX])
                rec[RSUM] += val[RSUM]
                rec[RCNT] += val[RCNT]
            else:
                target[key] = val
    return target


def output_data(out_file: str, data: dict[str, Record]) -> None:
    with open(out_file, "w", encoding="utf-8") as file:
        print("{", ", ".join(rec_str(loc, val) for loc, val in sorted(data.items())), "}", sep="", file=file)


# For the chunk_size, we seem to bottom out at 64MB. 128MB, 32MB are slower - but not by much.
def get_chunk_info(
    file_name: str, chunk_size: int = 32 * 1024 * 1024, buf_size: int = 4096
) -> list[tuple[str, int, int, int]]:
    assert chunk_size > 0, "Chunk target_size must be positive"
    results = []
    buffer = bytearray(buf_size)

    with open(file_name, "rb") as file:
        # Get the file size
        file.seek(0, 2)
        file_size = file.tell()
        # Reset the file pointer
        file.seek(0, 0)
        id = 0
        offset = 0

        while offset < file_size:
            id += 1
            if offset + chunk_size >= file_size:
                results.append((file_name, id, offset, file_size - offset))
                break
            else:
                file.seek(offset + chunk_size)
                file.readinto(buffer)
                newline_loc = buffer.find(b"\n")
                assert newline_loc != -1, f"No end of line found following {offset=:,}"
                results.append((file_name, id, offset, chunk_size + newline_loc + 1))
                offset += chunk_size + newline_loc + 1
    return results


def process_chunk(chunk: bytes, id=int) -> dict[str, list]:
    result: dict[str, list] = {}
    size: int = len(chunk)
    line_start: int = 0
    while line_start < size:
        name_end = chunk.find(b";", line_start)
        # assert name_end != -1, f"Separator not found after offset {line_start}."
        # It seems to be OK(ish) to skip the decoding step here. We don't really need it until we write the output.
        station = chunk[line_start:name_end]  # .decode(encoding="utf-8")
        temp = parse_temp(chunk, name_end + 1)
        # Add data for this line
        if station not in result:
            # min, max, sum, count
            result[station] = [temp, temp, temp, 1]
        else:
            rec = result[station]
            rec[RMIN] = min(rec[RMIN], temp)
            rec[RMAX] = max(rec[RMAX], temp)
            rec[RSUM] += temp
            rec[RCNT] += 1
        # Find next line
        line_start = chunk.find(b"\n", name_end)  # could catch the end of temp and start there
        # assert line_start != -1, f"No newline at end of chunk {id} following {name_end=}"
        line_start += 1  # eat the newline
    return result


def read_and_process_chunk(in_file: str, id: int, offset: int, size: int) -> dict[str, list]:
    with open(in_file, "rb") as file:
        file.seek(offset)
        chunk = file.read(size)
        return process_chunk(chunk, id)


def main(input: str, output: str, workers: int, chunk_size: int, verbose: bool = False) -> None:
    t0 = time.time()
    chunk_info = get_chunk_info(input, chunk_size=chunk_size)
    t1 = time.time()
    with Pool(workers) as pool:
        results = pool.starmap(read_and_process_chunk, chunk_info)
    t2 = time.time()
    merged = merge_data(results[0], results[1:])
    t3 = time.time()
    print("{", ", ".join(rec_str(loc, val) for loc, val in sorted(merged.items())), "}", sep="")
    # output_data(output, merged)
    t4 = time.time()
    if verbose:
        import sys

        print(
            f"Chunks: {len(chunk_info)}, CPUs: {workers}/{os.cpu_count()}, Process: {t2-t1:.2f}s, Merge: {t3-t2:.2f}s, Output: {t4-t3:.2f}s",
            file=sys.stderr,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some temperatures.")
    parser.add_argument(
        "--input",
        type=str,
        help="Input file path.",
        default="data/measurements.txt",
    )
    parser.add_argument("--output", type=str, help="Output file path", default="output.csv")
    parser.add_argument("--workers", type=int, help="Number of subprocesses", default=max(1, os.cpu_count() - 1))
    parser.add_argument("--chunk_size", type=int, help="Chunk size in bytes", default=64 * 1024 * 1024)
    parser.add_argument("--verbose", type=bool, help="Suppress extra output", default=False)

    args = parser.parse_args()
    main(args.input, args.output, args.workers, args.chunk_size, verbose=args.verbose)
