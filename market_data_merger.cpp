// market_data_merger.cpp
#include "market_data_merger.h"
#include <filesystem>
#include <iostream>
#include <algorithm>

namespace fs = std::filesystem;

MarketDataMerger::MarketDataMerger(const std::string& inputDir, const std::string& tempDir, const std::string& outputFile)
    : inputDir_(inputDir), tempDir_(tempDir), outputFile_(outputFile) {
}

void MarketDataMerger::merge() {
    // Get all input files
    std::vector<std::string> allFiles = getInputFiles();
    if (allFiles.empty()) {
        std::cerr << "No input files found in " << inputDir_ << std::endl;
        return;
    }

    // Process in groups of MAX_FILES_OPEN
    size_t numGroups = (allFiles.size() + MAX_FILES_OPEN - 1) / MAX_FILES_OPEN;
    std::vector<std::string> tempFiles;

    for (size_t i = 0; i < numGroups; ++i) {
        size_t start = i * MAX_FILES_OPEN;
        size_t end = std::min(start + MAX_FILES_OPEN, allFiles.size());
        std::vector<std::string> groupFiles(allFiles.begin() + start, allFiles.begin() + end);
        std::string tempOutput = tempDir_ + "/temp_" + std::to_string(i) + ".txt";
        mergeGroup(groupFiles, tempOutput);
        tempFiles.push_back(tempOutput);
    }

    // Merge temporary files into final output
    mergeTemporaryFiles(tempFiles, outputFile_);

    // Clean up temporary files
    for (const auto& tempFile : tempFiles) {
        fs::remove(tempFile);
    }
}

void MarketDataMerger::mergeGroup(const std::vector<std::string>& files, const std::string& outputFile) {
    std::priority_queue<MarketEntry, std::vector<MarketEntry>, CompareMarketEntry> minHeap;
    std::vector<std::ifstream> fileStreams(files.size());

    // Open files and populate heap with first entries
    for (size_t i = 0; i < files.size(); ++i) {
        fileStreams[i].open(files[i]);
        if (!fileStreams[i].is_open()) {
            std::cerr << "Failed to open " << files[i] << std::endl;
            continue;
        }
        std::string line;
        if (std::getline(fileStreams[i], line)) { // Skip header
            if (std::getline(fileStreams[i], line)) {
                size_t pos = line.find(',');
                if (pos != std::string::npos) {
                    std::string timestamp = line.substr(0, pos);
                    std::string symbol = extractSymbol(files[i]);
                    minHeap.emplace(timestamp, symbol, line, i);
                }
            }
        }
    }

    std::ofstream outFile(outputFile);
    if (!outFile.is_open()) {
        std::cerr << "Failed to open " << outputFile << std::endl;
        return;
    }

    // Merge entries
    while (!minHeap.empty()) {
        MarketEntry entry = minHeap.top();
        minHeap.pop();
        outFile << entry.symbol << "," << entry.line << "\n";

        // Declare 'line' in this scope
        std::string line;
        if (std::getline(fileStreams[entry.fileIndex], line)) {
            size_t pos = line.find(',');
            if (pos != std::string::npos) {
                std::string timestamp = line.substr(0, pos);
                minHeap.emplace(timestamp, entry.symbol, line, entry.fileIndex);
            }
        }
    }

    for (auto& fs : fileStreams) {
        if (fs.is_open()) fs.close();
    }
}

void MarketDataMerger::mergeTemporaryFiles(const std::vector<std::string>& tempFiles, const std::string& finalOutput) {
    std::priority_queue<MarketEntry, std::vector<MarketEntry>, CompareMarketEntry> minHeap;
    std::vector<std::ifstream> fileStreams(tempFiles.size());

    // Open temp files and populate heap
    for (size_t i = 0; i < tempFiles.size(); ++i) {
        fileStreams[i].open(tempFiles[i]);
        if (!fileStreams[i].is_open()) {
            std::cerr << "Failed to open " << tempFiles[i] << std::endl;
            continue;
        }
        std::string line;
        if (std::getline(fileStreams[i], line)) {
            size_t firstComma = line.find(',');
            size_t secondComma = line.find(',', firstComma + 1);
            if (firstComma != std::string::npos && secondComma != std::string::npos) {
                std::string symbol = line.substr(0, firstComma);
                std::string timestamp = line.substr(firstComma + 1, secondComma - firstComma - 1);
                minHeap.emplace(timestamp, symbol, line, i);
            }
        }
    }

    std::ofstream outFile(finalOutput);
    if (!outFile.is_open()) {
        std::cerr << "Failed to open " << finalOutput << std::endl;
        return;
    }
    outFile << "Symbol,Timestamp,Price,Size,Exchange,Type\n"; // Write header

    // Merge entries
    while (!minHeap.empty()) {
        MarketEntry entry = minHeap.top();
        minHeap.pop();
        outFile << entry.line << "\n";

        // Declare 'line' in this scope
        std::string line;
        if (std::getline(fileStreams[entry.fileIndex], line)) {
            size_t firstComma = line.find(',');
            size_t secondComma = line.find(',', firstComma + 1);
            if (firstComma != std::string::npos && secondComma != std::string::npos) {
                std::string symbol = line.substr(0, firstComma);
                std::string timestamp = line.substr(firstComma + 1, secondComma - firstComma - 1);
                minHeap.emplace(timestamp, symbol, line, entry.fileIndex);
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

std::vector<std::string> MarketDataMerger::getInputFiles() const {
    std::vector<std::string> files;
    for (const auto& entry : fs::directory_iterator(inputDir_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".txt") {
            files.push_back(entry.path().string());
        }
    }
    std::sort(files.begin(), files.end()); // Ensure consistent ordering
    return files;
}