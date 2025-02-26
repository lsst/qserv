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
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "boost/program_options.hpp"

#include "partition/ParquetInterface.h"

namespace po = boost::program_options;
namespace part = lsst::partition;

using namespace std;

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
                "  sph-parq2csv [options] <parq-file> <coldef-file> <csv-file>\n\n"
                "Options";

        po::options_description desc(help, 80);
        desc.add_options()("help,h", "Produce this help");
        desc.add_options()("verbose,v", "Produce verbose output.");
        desc.add_options()("max-proc-mem-mb", po::value<int>()->default_value(maxMemAllocatedMB),
                           "Max size (MB) of RAM allocated to the process.");
        desc.add_options()("buf-size-mb", po::value<int>()->default_value(maxBuffSizeMB),
                           "Buffers size (MB) for translating batches.");
        desc.add_options()("parq-file", po::value<vector<string>>(), "Input file to be translated.");
        desc.add_options()("coldef-file", po::value<vector<string>>(),
                           "Input file with the names of columns to be extracted.");
        desc.add_options()("csv-file", po::value<vector<string>>(), "Output file to be written.");

        po::positional_options_description p;
        p.add("parq-file", 1);
        p.add("coldef-file", 1);
        p.add("csv-file", 1);

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
        po::notify(vm);

        if (vm.count("help")) {
            cout << desc << "\n";
            return false;
        }
        parqFileName = vm.count("parq-file") ? vm["parq-file"].as<vector<string>>().front() : string();
        coldefFileName = vm.count("coldef-file") ? vm["coldef-file"].as<vector<string>>().front() : string();
        csvFileName = vm.count("csv-file") ? vm["csv-file"].as<vector<string>>().front() : string();

        if (parqFileName.empty() || coldefFileName.empty() || csvFileName.empty()) {
            throw runtime_error("The names of all required files must be provided.");
        }
        if (csvFileName == parqFileName) {
            throw runtime_error("Input and output file names must be different.");
        }
        _parseColdDefFile();

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

        return true;
    }

    // Values of the parsed parameters aere stored in the data members defined below.

    string parqFileName;
    string coldefFileName;
    string csvFileName;

    vector<string> columns;
    map<string, string> optionalColumnDefs;

    int maxMemAllocatedMB = 3000;
    int maxBuffSizeMB = 16;

    string const nullStr = "\\N";
    string const delimStr = "\t";

    bool verbose = false;

private:
    void _parseColdDefFile() {
        columns.clear();
        ifstream columnsFile(coldefFileName);
        if (!columnsFile) {
            throw runtime_error("Error while opening the columns file.");
        }
        string column;
        while (columnsFile >> column) {
            columns.push_back(column);
        }
        if (columns.empty()) {
            throw runtime_error("No columns to be extracted.");
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
        if (parqFile.setupBatchReader(maxBuffSizeBytes) != arrow::Status::OK()) {
            throw runtime_error("Error while setting up the batch reader.");
        }
        ofstream csvFile(params.csvFileName, ios::out | ios::binary);
        if (!csvFile) {
            throw runtime_error("Error while opening the output file.");
        }
        while (true) {
            auto status = parqFile.readNextBatch_Table2CSV(buf.get(), buffSize, params.columns,
                                                           params.optionalColumnDefs, params.nullStr,
                                                           params.delimStr);
            if ((status != arrow::Status::OK()) || (buffSize == 0)) break;
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
