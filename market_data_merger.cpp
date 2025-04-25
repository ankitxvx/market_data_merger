#include "market_data_merger.h"
#include <filesystem>
#include <iostream>
#include <algorithm>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <fstream>

namespace fs = std::filesystem;
 

std::mutex cerr_mutex;

 
MarketDataMerger::MarketDataMerger(const std::string& inputDir, const std::string& tempDir, const std::string& outputFile)
    : inputDir_(inputDir), tempDir_(tempDir), outputFile_(outputFile) {
    if (!fs::exists(tempDir_)) {
        fs::create_directories(tempDir_);
    }
}

// Main merge function: orchestrates the merging process
void MarketDataMerger::merge() {
    std::vector<std::string> allFiles = getInputFiles();
    if (allFiles.empty()) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "No input files found in " << inputDir_ << std::endl;
        return;
    }
    std::cout << "Found " << allFiles.size() << " input files." << std::endl;

    // Split files into groups to avoid exceeding max open files
    size_t numGroups = (allFiles.size() + MAX_FILES_OPEN - 1) / MAX_FILES_OPEN;
    std::vector<std::string> tempFiles(numGroups);
    std::vector<std::thread> threads;

    std::cout << "Starting initial merge phase with " << numGroups << " groups..." << std::endl;

    // Launch threads to merge each group in parallel
    for (size_t i = 0; i < numGroups; ++i) {
        size_t start = i * MAX_FILES_OPEN;
        size_t end = std::min(start + MAX_FILES_OPEN, allFiles.size());
        std::vector<std::string> groupFiles(allFiles.begin() + start, allFiles.begin() + end);
        std::string tempOutput = (fs::path(tempDir_) / ("temp_" + std::to_string(i) + ".txt")).string();
        tempFiles[i] = tempOutput;

        threads.emplace_back(&MarketDataMerger::mergeGroup, this, std::move(groupFiles), std::move(tempOutput));
    }

    
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    std::cout << "Initial merge phase completed. Merging temporary files..." << std::endl;

    // Merge all temporary files into the final output
    mergeTemporaryFiles(tempFiles, outputFile_);

    std::cout << "Final merge completed. Cleaning up temporary files..." << std::endl;

     
    for (const auto& tempFile : tempFiles) {
         try {
             if (fs::exists(tempFile)) {
                 fs::remove(tempFile);
             }
         } catch (const fs::filesystem_error& e) {
             std::lock_guard<std::mutex> lock(cerr_mutex);
             std::cerr << "Warning: Failed to remove temporary file " << tempFile << ": " << e.what() << std::endl;
         }
    }
     std::cout << "Cleanup finished." << std::endl;
}

// Merges a group of files into a single sorted temporary file
void MarketDataMerger::mergeGroup(const std::vector<std::string>& files, const std::string& outputFile) {
    std::priority_queue<MarketEntry, std::vector<MarketEntry>, CompareMarketEntry> minHeap;
    std::vector<std::ifstream> fileStreams(files.size());

    
    for (size_t i = 0; i < files.size(); ++i) {
        fileStreams[i].open(files[i]);
        if (!fileStreams[i].is_open()) {
            {
                std::lock_guard<std::mutex> lock(cerr_mutex);
                std::cerr << "Warning: Failed to open input file " << files[i] << " in thread " << std::this_thread::get_id() << std::endl;
            }
            continue;
        }
        std::string headerLine;
        std::string firstDataLine;
        if (std::getline(fileStreams[i], headerLine)) {
            if (std::getline(fileStreams[i], firstDataLine)) {
                size_t pos = firstDataLine.find(',');
                if (pos != std::string::npos) {
                    std::string timestamp = firstDataLine.substr(0, pos);
                    std::string symbol = extractSymbol(files[i]);
                    minHeap.emplace(timestamp, symbol, firstDataLine, i);
                } else {
                     {
                         std::lock_guard<std::mutex> lock(cerr_mutex);
                         std::cerr << "Warning: Invalid format in first data line of file " << files[i] << " in thread " << std::this_thread::get_id() << std::endl;
                     }
                }
            }
        }
    }

    std::ofstream outFile(outputFile);
    if (!outFile.is_open()) {
         {
             std::lock_guard<std::mutex> lock(cerr_mutex);
             std::cerr << "Error: Failed to open temporary output file " << outputFile << " in thread " << std::this_thread::get_id() << std::endl;
         }
        for (auto& fs : fileStreams) {
            if (fs.is_open()) fs.close();
        }
        return;
    }

    // Merge lines from all files using a min-heap
    while (!minHeap.empty()) {
        MarketEntry smallestEntry = minHeap.top();
        minHeap.pop();

        outFile << smallestEntry.symbol << "," << smallestEntry.line << "\n";

        std::string nextLine;
        size_t fileIdx = smallestEntry.fileIndex;
        if (fileStreams[fileIdx].good()) {
            if (std::getline(fileStreams[fileIdx], nextLine)) {
                size_t pos = nextLine.find(',');
                if (pos != std::string::npos) {
                    std::string timestamp = nextLine.substr(0, pos);
                    minHeap.emplace(timestamp, smallestEntry.symbol, nextLine, fileIdx);
                } else {
                     {
                         std::lock_guard<std::mutex> lock(cerr_mutex);
                         std::cerr << "Warning: Invalid line format encountered in file index " << fileIdx << " (Symbol: " << smallestEntry.symbol << ") in thread " << std::this_thread::get_id() << ": " << nextLine << std::endl;
                     }
                }
            } else if (!fileStreams[fileIdx].eof()) {
                 {
                     std::lock_guard<std::mutex> lock(cerr_mutex);
                     std::cerr << "Warning: Failed to read next line from input file index " << fileIdx << " (Symbol: " << smallestEntry.symbol << ") in thread " << std::this_thread::get_id() << ". Stream state: " << fileStreams[fileIdx].rdstate() << std::endl;
                 }
            }
        }
    }

    
    for (auto& fs : fileStreams) {
        if (fs.is_open()) fs.close();
    }
}

// Merges all temporary files into the final output file
void MarketDataMerger::mergeTemporaryFiles(const std::vector<std::string>& tempFiles, const std::string& finalOutput) {
    std::priority_queue<MarketEntry, std::vector<MarketEntry>, CompareMarketEntry> minHeap;
    std::vector<std::ifstream> fileStreams(tempFiles.size());

    // Open each temp file and push the first line into the heap
    for (size_t i = 0; i < tempFiles.size(); ++i) {
        if (!fs::exists(tempFiles[i]) || fs::is_empty(tempFiles[i])) {
             {
                 std::lock_guard<std::mutex> lock(cerr_mutex);
                 std::cerr << "Warning: Temporary file " << tempFiles[i] << " is missing or empty. Skipping." << std::endl;
             }
            continue;
        }

        fileStreams[i].open(tempFiles[i]);
        if (!fileStreams[i].is_open()) {
             {
                 std::lock_guard<std::mutex> lock(cerr_mutex);
                 std::cerr << "Error: Failed to open temporary file " << tempFiles[i] << std::endl;
             }
            continue;
        }

        std::string line;
        if (std::getline(fileStreams[i], line)) {
            size_t firstComma = line.find(',');
            size_t secondComma = (firstComma != std::string::npos) ? line.find(',', firstComma + 1) : std::string::npos;

            if (firstComma != std::string::npos && secondComma != std::string::npos) {
                std::string symbol = line.substr(0, firstComma);
                std::string timestamp = line.substr(firstComma + 1, secondComma - firstComma - 1);
                minHeap.emplace(timestamp, symbol, line, i);
            } else {
                 {
                     std::lock_guard<std::mutex> lock(cerr_mutex);
                     std::cerr << "Warning: Invalid format in first line of temporary file " << tempFiles[i] << ": " << line << std::endl;
                 }
            }
        } else if (!fileStreams[i].eof()) {
             {
                 std::lock_guard<std::mutex> lock(cerr_mutex);
                 std::cerr << "Warning: Failed to read first line from temporary file " << tempFiles[i] << std::endl;
             }
        }
    }

    std::ofstream outFile(finalOutput);
    if (!outFile.is_open()) {
         {
             std::lock_guard<std::mutex> lock(cerr_mutex);
             std::cerr << "Error: Failed to open final output file " << finalOutput << std::endl;
         }
        for (auto& fs : fileStreams) {
            if (fs.is_open()) fs.close();
        }
        return;
    }

     
    outFile << "Symbol,Timestamp,Price,Size,Exchange,Type\n";

    // Merge lines from all temp files using a min-heap
    while (!minHeap.empty()) {
        MarketEntry smallestEntry = minHeap.top();
        minHeap.pop();

        outFile << smallestEntry.line << "\n";

        std::string nextLine;
        size_t fileIdx = smallestEntry.fileIndex;
        if (fileStreams[fileIdx].good()) {
             if (std::getline(fileStreams[fileIdx], nextLine)) {
                size_t firstComma = nextLine.find(',');
                size_t secondComma = (firstComma != std::string::npos) ? nextLine.find(',', firstComma + 1) : std::string::npos;

                if (firstComma != std::string::npos && secondComma != std::string::npos) {
                    std::string symbol = nextLine.substr(0, firstComma);
                    std::string timestamp = nextLine.substr(firstComma + 1, secondComma - firstComma - 1);
                    minHeap.emplace(timestamp, symbol, nextLine, fileIdx);
                } else {
                     {
                         std::lock_guard<std::mutex> lock(cerr_mutex);
                         std::cerr << "Warning: Invalid line format encountered in temporary file index " << fileIdx << ": " << nextLine << std::endl;
                     }
                }
            } else if (!fileStreams[fileIdx].eof()) {
                 {
                     std::lock_guard<std::mutex> lock(cerr_mutex);
                     std::cerr << "Warning: Failed to read next line from temporary file index " << fileIdx << ". Stream state: " << fileStreams[fileIdx].rdstate() << std::endl;
                 }
            }
        }
    }

    
    for (auto& fs : fileStreams) {
        if (fs.is_open()) fs.close();
    }
}

 
std::string MarketDataMerger::extractSymbol(const std::string& filePath) const {
    return fs::path(filePath).stem().string();
}

// Returns a sorted list of input .txt files from the input directory
std::vector<std::string> MarketDataMerger::getInputFiles() const {
    std::vector<std::string> files;
    try {
        if (!fs::exists(inputDir_) || !fs::is_directory(inputDir_)) {
             {
                 std::lock_guard<std::mutex> lock(cerr_mutex);
                 std::cerr << "Error: Input directory " << inputDir_ << " does not exist or is not a directory." << std::endl;
             }
            return files;
        }

        for (const auto& entry : fs::directory_iterator(inputDir_)) {
            if (entry.is_regular_file() && entry.path().extension() == ".txt") {
                files.push_back(entry.path().string());
            }
        }
        std::sort(files.begin(), files.end());
    } catch (const fs::filesystem_error& e) {
         {
             std::lock_guard<std::mutex> lock(cerr_mutex);
             std::cerr << "Error accessing input directory " << inputDir_ << ": " << e.what() << std::endl;
         }
        files.clear();
    }
    return files;
}