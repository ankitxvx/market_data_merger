// market_data_merger.h
#ifndef MARKET_DATA_MERGER_H
#define MARKET_DATA_MERGER_H

#include <string>
#include <vector>
#include <queue>
#include <fstream>

// Structure to hold an entry in the min-heap
struct MarketEntry {
    std::string timestamp;
    std::string symbol;
    std::string line;
    size_t fileIndex;

    MarketEntry(const std::string& ts, const std::string& sym, const std::string& l, size_t idx)
        : timestamp(ts), symbol(sym), line(l), fileIndex(idx) {
    }
};

// Custom comparator for the min-heap: sorts by timestamp, then symbol
struct CompareMarketEntry {
    bool operator()(const MarketEntry& a, const MarketEntry& b) const {
        if (a.timestamp == b.timestamp) {
            return a.symbol > b.symbol; // Alphabetical order for equal timestamps
        }
        return a.timestamp > b.timestamp;
    }
};

class MarketDataMerger {
public:
    MarketDataMerger(const std::string& inputDir, const std::string& tempDir, const std::string& outputFile);
    void merge();

private:
    std::string inputDir_;
    std::string tempDir_;
    std::string outputFile_;
    static const size_t MAX_FILES_OPEN = 500; // Constraint on simultaneous file opens

    // Merges a group of files into a temporary file
    void mergeGroup(const std::vector<std::string>& files, const std::string& outputFile);

    // Merges temporary files into the final output
    void mergeTemporaryFiles(const std::vector<std::string>& tempFiles, const std::string& finalOutput);

    // Extracts symbol from file path
    std::string extractSymbol(const std::string& filePath) const;

    // Gets all .txt files from the input directory
    std::vector<std::string> getInputFiles() const;
};

#endif // MARKET_DATA_MERGER_H