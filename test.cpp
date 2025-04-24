// // test.cpp
// #include "market_data_merger.h"
// #include <iostream>
// #include <fstream>
// #include <cassert>
// #include <filesystem>

// namespace fs = std::filesystem;

// void createTestFile(const std::string& path, const std::vector<std::string>& lines) {
//     // Ensure the directory exists
//     fs::path filePath(path);
//     fs::create_directories(filePath.parent_path()); // Create parent directories if they don't exist

//     std::ofstream file(path);
//     if (!file.is_open()) {
//         std::cerr << "Failed to open file for writing: " << path << std::endl;
//         std::abort(); // Explicitly abort with a message
//     }
//     file << "Timestamp,Price,Size,Exchange,Type\n";
//     for (const auto& line : lines) {
//         file << line << "\n";
//     }
//     file.close();
// }

// void runTests() {
//     // Create directories if they don't exist
//     fs::create_directories("test_input");
//     fs::create_directories("test_temp");

//     // Create test input files
//     createTestFile("test_input/MSFT.txt", {
//         "2021-03-05 10:00:00.123,228.5,120,NYSE,Ask",
//         "2021-03-05 10:00:00.133,228.5,120,NYSE,TRADE"
//     });
//     createTestFile("test_input/CSCO.txt", {
//         "2021-03-05 10:00:00.123,46.14,120,NYSE_ARCA,Ask",
//         "2021-03-05 10:00:00.130,46.13,120,NYSE,TRADE"
//     });

//     // Run merger
//     MarketDataMerger merger("test_input", "test_temp", "test_output.txt");
//     merger.merge();

//     // Verify output
//     std::ifstream outFile("test_output.txt");
//     if (!outFile.is_open()) {
//         std::cerr << "Failed to open test_output.txt for reading" << std::endl;
//         std::abort();
//     }
//     std::string line;
//     std::vector<std::string> expected = {
//         "Symbol,Timestamp,Price,Size,Exchange,Type",
//         "CSCO,2021-03-05 10:00:00.123,46.14,120,NYSE_ARCA,Ask",
//         "MSFT,2021-03-05 10:00:00.123,228.5,120,NYSE,Ask",
//         "CSCO,2021-03-05 10:00:00.130,46.13,120,NYSE,TRADE",
//         "MSFT,2021-03-05 10:00:00.133,228.5,120,NYSE,TRADE"
//     };
//     size_t i = 0;
//     while (std::getline(outFile, line)) {
//         assert(i < expected.size() && line == expected[i]);
//         ++i;
//     }
//     assert(i == expected.size());
//     std::cout << "All tests passed!" << std::endl;
// }

// int main() {
//     runTests();
//     return 0;
// }