# Market Data Merger

`market_data_merger` is a C++ utility that merges many per-symbol market data files into a single, globally sorted output file.

The merger reads `.txt` files from an input directory (for example `MSFT.txt`, `CSCO.txt`), then performs a k-way merge ordered by:

1. `Timestamp` (ascending)
2. `Symbol` (alphabetical) when timestamps are equal

The final result is written in this format:

```text
Symbol,Timestamp,Price,Size,Exchange,Type
CSCO,2021-03-05 10:00:00.123,46.14,120,NYSE_ARCA,Ask
MSFT,2021-03-05 10:00:00.123,228.5,120,NYSE,Ask
...
```

## How it works

The merger is implemented in two phases to avoid opening too many files at once:

- **Phase 1: Group merge**
  - Input files are split into groups of up to `MAX_FILES_OPEN` (currently `500`).
  - Each group is merged into a temporary file.
- **Phase 2: Final merge**
  - All temporary files are merged into the final output file.

Internally, each merge uses a min-heap (`std::priority_queue` with custom comparator) for efficient k-way merging.

## Input format

Each input file should:

- Be a `.txt` file in the input directory.
- Be named after its symbol (for example `AAPL.txt`).
- Contain a header row followed by market data rows:

```text
Timestamp,Price,Size,Exchange,Type
2021-03-05 10:00:00.123,228.5,120,NYSE,Ask
2021-03-05 10:00:00.133,228.5,120,NYSE,TRADE
```

## Build

Compile with any C++17-compatible compiler.

### g++ example

```bash
g++ -std=c++17 -O2 -Wall -Wextra -pedantic main.cpp market_data_merger.cpp -o market_data_merger
```

## Run

```bash
./market_data_merger <input_dir> <temp_dir> <output_file>
```

Example:

```bash
mkdir -p temp
./market_data_merger input_dir temp output.txt
```

Program usage message:

```text
Usage: ./market_data_merger <input_dir> <temp_dir> <output_file>
```

## Example with repository sample data

```bash
mkdir -p temp
./market_data_merger input_dir temp merged_output.txt
cat merged_output.txt
```

## Notes and limitations

- Input rows are expected to be valid CSV-like lines with commas.
- The parser extracts timestamps as string prefixes up to the first comma.
- Ordering assumes timestamp strings are lexicographically sortable in chronological order (for example `YYYY-MM-DD HH:MM:SS.mmm`).
- The program expects the temporary directory to exist before execution.

## Project structure

- `main.cpp` — CLI entry point.
- `market_data_merger.h` — data structures and class interface.
- `market_data_merger.cpp` — merge implementation.
- `input_dir/` — sample input files.
- `test_input/`, `test_output.txt` — sample test artifacts.

