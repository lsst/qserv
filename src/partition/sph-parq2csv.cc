/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#include <iomanip>
#include <iostream>
#include <fstream>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "boost/program_options.hpp"
#include "nlohmann/json.hpp"

#include "partition/ParquetInterface.h"

namespace po = boost::program_options;
namespace part = lsst::partition;

using namespace std;
using json = nlohmann::json;

namespace {

/**
 * This helper class is used to parse the command line parameters.
 */
class CommandLineParams {
public:
    /**
     * Parse the command line parameters.
     * @param argc - number of parameters
     * @param argv - list of parameters
     * @returns true if the parameters are parsed successfully, false otherwise.
     */
    bool parse(int argc, char const* const* argv) {
        static char const* help =
                "The tool for translating Parquet files into CSV.\n\n"
                "Usage:\n"
                "  sph-parq2csv [options] --parq=<file> --config=<file> --csv=<file>\n\n"
                "Options";

        po::options_description desc(help, 80);
        desc.add_options()("help,h", "Produce this help");
        desc.add_options()("verbose,v", "Produce verbose output.");
        desc.add_options()("csv-quote-fields", "Double quote fields as needed in the generated CSV.");
        desc.add_options()("max-proc-mem-mb", po::value<int>()->default_value(maxMemAllocatedMB),
                           "Max size (MB) of RAM allocated to the process.");
        desc.add_options()("buf-size-mb", po::value<int>()->default_value(maxBuffSizeMB),
                           "Buffers size (MB) for translating batches.");
        desc.add_options()("parq", po::value<vector<string>>(), "Input file to be translated.");
        desc.add_options()("config", po::value<vector<string>>(),
                           "Input JSON file with definition of columns to be extracted.");
        desc.add_options()("csv", po::value<vector<string>>(), "Output file to be written.");

        po::positional_options_description p;
        p.add("parq", 1);
        p.add("config", 1);
        p.add("csv", 1);

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
        po::notify(vm);

        if (vm.count("help")) {
            cout << desc << "\n";
            return false;
        }
        parqFileName = vm.count("parq") ? vm["parq"].as<vector<string>>().front() : string();
        configFileName = vm.count("config") ? vm["config"].as<vector<string>>().front() : string();
        csvFileName = vm.count("csv") ? vm["csv"].as<vector<string>>().front() : string();

        if (parqFileName.empty() || configFileName.empty() || csvFileName.empty()) {
            throw runtime_error("The names of all required files must be provided.");
        }
        if (csvFileName == parqFileName) {
            throw runtime_error("Input and output file names must be different.");
        }
        _parseConfigFile();

        if (vm.count("max-mem-alloc-mb")) {
            maxMemAllocatedMB = vm["max-mem-alloc-mb"].as<int>();
        }
        if (maxMemAllocatedMB <= 1) {
            throw runtime_error("Memory allocation must be equal to 1 or greater.");
        }
        if (vm.count("buf-size-mb")) {
            maxBuffSizeMB = vm["buf-size-mb"].as<int>();
        }
        if ((maxBuffSizeMB < 1) || (maxBuffSizeMB > 1024)) {
            throw runtime_error("Buffer size (MB) must be in a range of [1,1024].");
        }
        verbose = vm.count("verbose") != 0;
        quote = vm.count("csv-quote-fields") != 0;

        return true;
    }

    // Values of the parsed parameters aere stored in the data members defined below.

    string parqFileName;
    string configFileName;
    string csvFileName;

    vector<string> columns;
    set<string> optionalColumns;

    int maxMemAllocatedMB = 3000;
    int maxBuffSizeMB = 16;

    string const nullStr = "\\N";
    string const delimStr = "\t";

    bool verbose = false;
    bool quote = false;

private:
    void _parseConfigFile() {
        ifstream file(configFileName, ios_base::in);
        if (!file.good()) throw invalid_argument("Failed to open file: '" + configFileName + "'");
        json config;
        try {
            file >> config;
        } catch (...) {
            throw runtime_error("Config file: '" + configFileName + "' doesn't have a valid JSON payload");
        }
        if (!config.is_object()) {
            throw invalid_argument("Config file: '" + configFileName + "' is not a valid JSON object");
        }
        if (!config.contains("columns")) {
            throw runtime_error("The JSON file must contain a 'columns' key.");
        }
        if (!config["columns"].is_array()) {
            throw runtime_error("The 'columns' key must contain an array.");
        }
        columns = config["columns"].get<vector<string>>();
        if (columns.empty()) {
            throw runtime_error("No columns to be extracted.");
        }
        optionalColumns.clear();
        if (config.contains("optional")) {
            if (!config["optional"].is_array()) {
                throw runtime_error("The 'optional' key must contain an object.");
            }
            for (auto const& column : config["optional"].get<vector<string>>()) {
                optionalColumns.insert(column);
            }
        }
        // All optional columns must be defined in the 'columns' array.
        for (auto const& name : optionalColumns) {
            if (find(columns.begin(), columns.end(), name) == columns.end()) {
                throw runtime_error("The optional column '" + name +
                                    "' is not defined in the 'columns' array.");
            }
        }
    }
};

}  // namespace

int main(int argc, char const* const* argv) {
    int buffSize;
    size_t numBytesWritten = 0;

    try {
        ::CommandLineParams params;
        if (!params.parse(argc, argv)) {
            return 1;
        }
        int const maxBuffSizeBytes = params.maxBuffSizeMB * 1024 * 1024;
        unique_ptr<char> buf(new char[maxBuffSizeBytes]);

        if (params.verbose) {
            cout << "Translating '" << params.parqFileName << "' into '" << params.csvFileName << "'" << endl;
        }
        part::ParquetFile parqFile(params.parqFileName, params.maxMemAllocatedMB);
        parqFile.setupBatchReader(maxBuffSizeBytes);

        ofstream csvFile(params.csvFileName, ios::out | ios::binary);
        if (!csvFile) {
            throw runtime_error("Error while opening the output file.");
        }
        while (true) {
            bool const success = parqFile.readNextBatch_Table2CSV(buf.get(), buffSize, params.columns,
                                                                  params.optionalColumns, params.nullStr,
                                                                  params.delimStr, params.quote);
            if (!success) break;
            if (buffSize == 0) {
                throw runtime_error("Received EOF while reading the file.");
            }
            if (params.verbose) {
                cout << "Writing " << setw(9) << buffSize << " bytes" << endl;
            }
            csvFile.write((char*)(buf.get()), buffSize);
            numBytesWritten += buffSize;
        }
        csvFile.close();
        if (params.verbose) {
            cout << "Wrote   " << setw(9) << numBytesWritten << " bytes" << endl;
        }
    } catch (exception const& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
    return 0;
}
