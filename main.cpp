// main.cpp
#include "market_data_merger.h"
#include <iostream>

int main(int argc, char* argv[]) {
   if (argc != 4) {
       std::cerr << "Usage: " << argv[0] << " <input_dir> <temp_dir> <output_file>" << std::endl;
       return 1;
   }

   std::string inputDir = argv[1];
   std::string tempDir = argv[2];
   std::string outputFile = argv[3];

   try {
       MarketDataMerger merger(inputDir, tempDir, outputFile);
       merger.merge();
       std::cout << "Merging completed successfully." << std::endl;
   }
   catch (const std::exception& e) {
       std::cerr << "Error: " << e.what() << std::endl;
       return 1;
   }

   return 0;
}