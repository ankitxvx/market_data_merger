# Market Data Merger

## Overview

This project provides a C++  program designed to merge multiple market data files from various stock symbols into a single, chronologically sorted output file. It's built to handle potentially large numbers of input files efficiently by using multi-threading and a two-phase merge strategy.

## Functionality

The primary goal is to take numerous input `.txt` files, each representing market data (trades, bids, asks) for a specific stock symbol and containing timestamped entries, and combine them into one consolidated `.txt` file. The final output file includes all entries from the input files, sorted globally by timestamp, with an added column indicating the source symbol for each entry.

**Input:**

*   A directory containing multiple `.txt` files.
*   Each `.txt` file should be named after the stock symbol it represents (e.g., `MSFT.txt`, `CSCO.txt`).
*   Each file is expected to have a header line followed by data lines.
*   Data lines should be comma-separated, with the **timestamp** as the first column (e.g., `Timestamp,Price,Size,Exchange,Type`).

**Output:**

*   A single `.txt` file containing all data from the input files.
*   The output file includes a header: `Symbol,Timestamp,Price,Size,Exchange,Type`.
*   Data rows are sorted primarily by the `Timestamp` column across all symbols.
*   A `Symbol` column is prepended to each data row, indicating the source file (and thus the stock symbol).

## Approach

The merging process employs a multi-threaded, two-phase external merge sort strategy to handle potentially large datasets and avoid limitations on the number of simultaneously open files:

1.  **Input File Discovery:** The application scans the specified input directory for all `.txt` files using `std::filesystem`.
2.  **Grouping:** The discovered input files are divided into smaller groups. The size of these groups is determined by a predefined constant (`MAX_FILES_OPEN`) to prevent exceeding the operating system's limit on open file descriptors.
3.  **Phase 1: Parallel Group Merging:**
    *   Multiple threads are launched, with each thread responsible for merging one group of input files.
    *   Within each thread, a **k-way merge** algorithm is implemented using a `std::priority_queue` (min-heap).
    *   The min-heap stores the next available line (along with its timestamp and source file index) from each file in the group.
    *   The thread repeatedly extracts the entry with the earliest timestamp from the heap, writes it to a temporary output file (prepending the symbol derived from the input filename), and reads the next line from the corresponding input file to add back to the heap.
    *   This results in several intermediate temporary files, each containing the sorted data from one group.
4.  **Phase 2: Final Merge:**
    *   Once all threads from Phase 1 complete, a final merge operation begins.
    *   Another k-way merge (again using a `std::priority_queue`) is performed on the temporary files generated in Phase 1.
    *   The process is similar: read the first line from each temp file, use the heap to find the earliest timestamped entry, write it to the final output file, and replenish the heap with the next line from the source temporary file.
    *   A header row is added to the final output file.
5.  **Cleanup:** After the final merge is complete, all temporary files created during Phase 1 are deleted.
6.  **Concurrency Control:** `std::mutex` is used to safely write error messages or warnings to `std::cerr` from multiple threads without interleaving output.

## Prerequisites

*   A C++17 compliant compiler (uses `std::filesystem`).
*   Microsoft Visual Studio (for building using the provided `.sln` file).

## How to Build

1.  Open the `market_data_merger.sln` file in Visual Studio.
2.  Select a build configuration (e.g., `Release`, `x64`).
3.  Build the solution (Build Menu -> Build Solution or press F7).
4.  The executable will be generated in the corresponding output directory (e.g., `x64/Release/`).

## How to Run

The application is run from the command line, providing paths for input, temporary storage, and the final output.

```bash
# Example usage:
./path/to/market_data_merger.exe <path_to_input_dir> <path_to_temp_dir> <path_to_output_file.txt>

# Example on Windows from the project root after building Release x64:
.\x64\Release\market_data_merger.exe .\input_dir .\temp_dir .\merged_output.txt
```

*   `<path_to_input_dir>`: Directory containing the symbol-named `.txt` files.
*   `<path_to_temp_dir>`: A directory where temporary merged files will be stored during processing. It will be created if it doesn't exist.
*   `<path_to_output_file.txt>`: The full path for the final merged and sorted output file.

## Directory Structure (Example)

```
Ankit_Vishwakarma_bestex_research/
|-- input_dir/             # Input files go here
|   |-- MSFT.txt
|   |-- CSCO.txt
|   |-- ...
|-- temp_dir/              # Temporary files (created & deleted by the app)
|-- x64/                   # Build output (created by Visual Studio)
|   |-- Debug/
|   |-- Release/
|       |-- market_data_merger.exe
|-- market_data_merger.sln # Visual Studio Solution File
|-- market_data_merger.vcxproj
|-- market_data_merger.vcxproj.filters
|-- market_data_merger.h   # Header file for the merger class
|-- market_data_merger.cpp # Implementation file for the merger class
|-- main.cpp               # Main executable entry point
|-- test.cpp               # Unit tests (if implemented)
|-- README.md              # This file
```

Ensure the `input_dir` exists and contains your data files before running. The `temp_dir` will be created if necessary.