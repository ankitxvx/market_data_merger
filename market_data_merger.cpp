// market_data_merger.cpp
#include "market_data_merger.h"
#include <filesystem>
#include <iostream>
#include <algorithm>
#include <vector> // Include vector
#include <thread> // Include thread
#include <mutex>  // Include mutex for thread-safe output

namespace fs = std::filesystem;

// Mutex for protecting console output (std::cerr) from concurrent access
std::mutex cerr_mutex;

MarketDataMerger::MarketDataMerger(const std::string& inputDir, const std::string& tempDir, const std::string& outputFile)
    : inputDir_(inputDir), tempDir_(tempDir), outputFile_(outputFile) {
    // Create temporary directory if it doesn't exist
    if (!fs::exists(tempDir_)) {
        fs::create_directories(tempDir_);
    }
}

void MarketDataMerger::merge() {
    // Get all input files
    std::vector<std::string> allFiles = getInputFiles();
    if (allFiles.empty()) {
        std::lock_guard<std::mutex> lock(cerr_mutex); // Lock before writing to cerr
        std::cerr << "No input files found in " << inputDir_ << std::endl;
        return;
    }
    std::cout << "Found " << allFiles.size() << " input files." << std::endl;

    // Process in groups of MAX_FILES_OPEN using multiple threads
    size_t numGroups = (allFiles.size() + MAX_FILES_OPEN - 1) / MAX_FILES_OPEN;
    std::vector<std::string> tempFiles(numGroups); // Pre-allocate space
    std::vector<std::thread> threads; // Vector to store threads

    std::cout << "Starting initial merge phase with " << numGroups << " groups..." << std::endl;

    for (size_t i = 0; i < numGroups; ++i) {
        size_t start = i * MAX_FILES_OPEN;
        size_t end = std::min(start + MAX_FILES_OPEN, allFiles.size());
        // Create a copy of the file paths for this group to pass to the thread
        std::vector<std::string> groupFiles(allFiles.begin() + start, allFiles.begin() + end);
        // Generate temporary file path (ensure thread-safe access if needed, but string creation is fine)
        std::string tempOutput = (fs::path(tempDir_) / ("temp_" + std::to_string(i) + ".txt")).string();
        tempFiles[i] = tempOutput; // Store temp file path

        // Launch a thread to merge this group
        // Pass arguments by value or ensure lifetimes are managed correctly.
        // 'this' pointer is passed to access the member function mergeGroup.
        // groupFiles and tempOutput are copied/moved into the thread's context.
        threads.emplace_back(&MarketDataMerger::mergeGroup, this, std::move(groupFiles), std::move(tempOutput));
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    std::cout << "Initial merge phase completed. Merging temporary files..." << std::endl;

    // Merge temporary files into final output (this remains single-threaded)
    mergeTemporaryFiles(tempFiles, outputFile_);

    std::cout << "Final merge completed. Cleaning up temporary files..." << std::endl;

    // Clean up temporary files
    for (const auto& tempFile : tempFiles) {
         try {
             if (fs::exists(tempFile)) {
                 fs::remove(tempFile);
             }
         } catch (const fs::filesystem_error& e) {
             std::lock_guard<std::mutex> lock(cerr_mutex); // Lock before writing to cerr
             std::cerr << "Warning: Failed to remove temporary file " << tempFile << ": " << e.what() << std::endl;
         }
    }
     std::cout << "Cleanup finished." << std::endl;
}

// Merges a group of input files into a single temporary output file.
// This function is now designed to be called concurrently by multiple threads.
void MarketDataMerger::mergeGroup(const std::vector<std::string>& files, const std::string& outputFile) {
    std::priority_queue<MarketEntry, std::vector<MarketEntry>, CompareMarketEntry> minHeap;
    std::vector<std::ifstream> fileStreams(files.size());

    // Open files and populate heap with first entries
    for (size_t i = 0; i < files.size(); ++i) {
        fileStreams[i].open(files[i]);
        if (!fileStreams[i].is_open()) {
            { // Lock scope for cerr
                std::lock_guard<std::mutex> lock(cerr_mutex);
                std::cerr << "Warning: Failed to open input file " << files[i] << " in thread " << std::this_thread::get_id() << std::endl;
            }
            continue; // Skip this file
        }
        std::string line;
        if (std::getline(fileStreams[i], line)) { // Skip header line
            if (std::getline(fileStreams[i], line)) { // Read the first data line
                size_t pos = line.find(',');
                if (pos != std::string::npos) {
                    std::string timestamp = line.substr(0, pos);
                    std::string symbol = extractSymbol(files[i]); // Symbol from filename
                    // Store the original line content along with symbol and timestamp for sorting
                    minHeap.emplace(timestamp, symbol, line, i);
                } else {
                     { // Lock scope for cerr
                         std::lock_guard<std::mutex> lock(cerr_mutex);
                         std::cerr << "Warning: Invalid format in first data line of file " << files[i] << " in thread " << std::this_thread::get_id() << std::endl;
                     }
                }
            }
        }
    }

    std::ofstream outFile(outputFile);
    if (!outFile.is_open()) {
         { // Lock scope for cerr
             std::lock_guard<std::mutex> lock(cerr_mutex);
             std::cerr << "Error: Failed to open temporary output file " << outputFile << " in thread " << std::this_thread::get_id() << std::endl;
         }
        // Close any opened input files before returning
        for (auto& fs : fileStreams) {
            if (fs.is_open()) fs.close();
        }
        return; // Cannot proceed if output file cannot be opened
    }

    // Merge entries using the min-heap
    while (!minHeap.empty()) {
        MarketEntry smallestEntry = minHeap.top();
        minHeap.pop();

        // Write the merged line in "Symbol,Timestamp,Price,Size,Exchange,Type" format
        outFile << smallestEntry.symbol << "," << smallestEntry.line << "\n";

        // Read the next line from the file the smallest entry came from
        std::string nextLine;
        size_t fileIdx = smallestEntry.fileIndex;
        // Check stream state *before* attempting getline
        if (fileStreams[fileIdx].good()) { // Use good() for a comprehensive check
            if (std::getline(fileStreams[fileIdx], nextLine)) {
                size_t pos = nextLine.find(',');
                if (pos != std::string::npos) {
                    std::string timestamp = nextLine.substr(0, pos);
                    // Re-use the symbol, add the new line content
                    minHeap.emplace(timestamp, smallestEntry.symbol, nextLine, fileIdx);
                } else {
                     { // Lock scope for cerr
                         std::lock_guard<std::mutex> lock(cerr_mutex);
                         std::cerr << "Warning: Invalid line format encountered in file index " << fileIdx << " (Symbol: " << smallestEntry.symbol << ") in thread " << std::this_thread::get_id() << ": " << nextLine << std::endl;
                     }
                }
            } else if (!fileStreams[fileIdx].eof()) { // Check if getline failed for a reason other than EOF
                 { // Lock scope for cerr
                     std::lock_guard<std::mutex> lock(cerr_mutex);
                     std::cerr << "Warning: Failed to read next line from input file index " << fileIdx << " (Symbol: " << smallestEntry.symbol << ") in thread " << std::this_thread::get_id() << ". Stream state: " << fileStreams[fileIdx].rdstate() << std::endl;
                 }
            }
            // If getline failed due to EOF, we correctly do nothing.
        }
        // If stream wasn't good() initially, the file likely had prior errors or was closed.
    }

    // Close all input file streams for this group
    for (auto& fs : fileStreams) {
        if (fs.is_open()) fs.close();
    }
    // Output file stream 'outFile' closes automatically when it goes out of scope
}

// Merges temporary files into the final output file.
// This part remains single-threaded as it operates on the results of the first phase.
void MarketDataMerger::mergeTemporaryFiles(const std::vector<std::string>& tempFiles, const std::string& finalOutput) {
    std::priority_queue<MarketEntry, std::vector<MarketEntry>, CompareMarketEntry> minHeap;
    std::vector<std::ifstream> fileStreams(tempFiles.size());

    // Open temporary files and populate heap with their first entries
    for (size_t i = 0; i < tempFiles.size(); ++i) {
        // Check if the temp file exists and is not empty before trying to open
        if (!fs::exists(tempFiles[i]) || fs::is_empty(tempFiles[i])) {
             { // Lock scope for cerr
                 std::lock_guard<std::mutex> lock(cerr_mutex);
                 std::cerr << "Warning: Temporary file " << tempFiles[i] << " is missing or empty. Skipping." << std::endl;
             }
            continue; // Skip this temp file
        }

        fileStreams[i].open(tempFiles[i]);
        if (!fileStreams[i].is_open()) {
             { // Lock scope for cerr
                 std::lock_guard<std::mutex> lock(cerr_mutex);
                 std::cerr << "Error: Failed to open temporary file " << tempFiles[i] << std::endl;
             }
            continue; // Skip this file
        }

        std::string line;
        // Read the first line (already includes symbol)
        if (std::getline(fileStreams[i], line)) {
            // Parse Symbol and Timestamp from the line (format: Symbol,Timestamp,...)
            size_t firstComma = line.find(',');
            size_t secondComma = (firstComma != std::string::npos) ? line.find(',', firstComma + 1) : std::string::npos;

            if (firstComma != std::string::npos && secondComma != std::string::npos) {
                std::string symbol = line.substr(0, firstComma);
                std::string timestamp = line.substr(firstComma + 1, secondComma - firstComma - 1);
                // Store the full line, symbol, timestamp, and file index
                minHeap.emplace(timestamp, symbol, line, i);
            } else {
                 { // Lock scope for cerr
                     std::lock_guard<std::mutex> lock(cerr_mutex);
                     std::cerr << "Warning: Invalid format in first line of temporary file " << tempFiles[i] << ": " << line << std::endl;
                 }
            }
        } else if (!fileStreams[i].eof()) {
             { // Lock scope for cerr
                 std::lock_guard<std::mutex> lock(cerr_mutex);
                 std::cerr << "Warning: Failed to read first line from temporary file " << tempFiles[i] << std::endl;
             }
        }
        // If getline fails and it's EOF, the file was likely empty or just had issues, already warned or handled.
    }

    std::ofstream outFile(finalOutput);
    if (!outFile.is_open()) {
         { // Lock scope for cerr
             std::lock_guard<std::mutex> lock(cerr_mutex);
             std::cerr << "Error: Failed to open final output file " << finalOutput << std::endl;
         }
        // Close any opened temp files before returning
        for (auto& fs : fileStreams) {
            if (fs.is_open()) fs.close();
        }
        return; // Cannot proceed
    }

    // Write the header to the final output file
    outFile << "Symbol,Timestamp,Price,Size,Exchange,Type\n";

    // Merge entries from temporary files
    while (!minHeap.empty()) {
        MarketEntry smallestEntry = minHeap.top();
        minHeap.pop();

        // Write the line (already contains the symbol) from the smallest entry
        outFile << smallestEntry.line << "\n";

        // Read the next line from the file the smallest entry came from
        std::string nextLine;
        size_t fileIdx = smallestEntry.fileIndex;
        // Check stream state *before* attempting getline
        if (fileStreams[fileIdx].good()) { // Use good() for a comprehensive check
             if (std::getline(fileStreams[fileIdx], nextLine)) {
                 // Parse Symbol and Timestamp from the next line
                size_t firstComma = nextLine.find(',');
                size_t secondComma = (firstComma != std::string::npos) ? nextLine.find(',', firstComma + 1) : std::string::npos;

                if (firstComma != std::string::npos && secondComma != std::string::npos) {
                    std::string symbol = nextLine.substr(0, firstComma);
                    std::string timestamp = nextLine.substr(firstComma + 1, secondComma - firstComma - 1);
                    minHeap.emplace(timestamp, symbol, nextLine, fileIdx);
                } else {
                     { // Lock scope for cerr
                         std::lock_guard<std::mutex> lock(cerr_mutex);
                         std::cerr << "Warning: Invalid line format encountered in temporary file index " << fileIdx << ": " << nextLine << std::endl;
                     }
                }
            } else if (!fileStreams[fileIdx].eof()) { // Check if getline failed for a reason other than EOF
                 { // Lock scope for cerr
                     std::lock_guard<std::mutex> lock(cerr_mutex);
                     std::cerr << "Warning: Failed to read next line from temporary file index " << fileIdx << ". Stream state: " << fileStreams[fileIdx].rdstate() << std::endl;
                 }
            }
             // If getline failed due to EOF, we correctly do nothing.
        }
         // If stream wasn't good() initially, the file likely had prior errors or was closed.
    }

    // Close all temporary file streams
    for (auto& fs : fileStreams) {
        if (fs.is_open()) fs.close();
    }
     // Final output file stream 'outFile' closes automatically
}

// Extracts symbol from file path (e.g., "C:/data/MSFT.txt" -> "MSFT")
// This function is const and inherently thread-safe for different inputs.
std::string MarketDataMerger::extractSymbol(const std::string& filePath) const {
    return fs::path(filePath).stem().string();
}

// Gets all .txt files from the input directory, sorted alphabetically.
// This is called once before threading begins.
std::vector<std::string> MarketDataMerger::getInputFiles() const {
    std::vector<std::string> files;
    try {
        if (!fs::exists(inputDir_) || !fs::is_directory(inputDir_)) {
             { // Lock scope for cerr
                 std::lock_guard<std::mutex> lock(cerr_mutex);
                 std::cerr << "Error: Input directory " << inputDir_ << " does not exist or is not a directory." << std::endl;
             }
            return files; // Return empty vector
        }

        for (const auto& entry : fs::directory_iterator(inputDir_)) {
            // Check if it's a regular file and has a .txt extension
            if (entry.is_regular_file() && entry.path().extension() == ".txt") {
                files.push_back(entry.path().string());
            }
        }
        std::sort(files.begin(), files.end()); // Ensure consistent ordering for symbol sorting later if needed
    } catch (const fs::filesystem_error& e) {
         { // Lock scope for cerr
             std::lock_guard<std::mutex> lock(cerr_mutex);
             std::cerr << "Error accessing input directory " << inputDir_ << ": " << e.what() << std::endl;
         }
        files.clear(); // Clear any partially collected files on error
    }
    return files;
}

// Definition for the static const member (if not defined elsewhere)
// const size_t MarketDataMerger::MAX_FILES_OPEN; // Typically defined in the header or here if needed