/*
 * LSST Data Management System
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

// Class header
#include "replica/apps/FileIngestApp.h"

// System headers
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <streambuf>
#include <iomanip>
#include <iostream>
#include <limits>
#include <regex>
#include <stdexcept>
#include <system_error>

// Qserv headers
#include "http/Auth.h"
#include "util/File.h"
#include "util/TimeUtils.h"

using namespace std;
using namespace nlohmann;
namespace fs = std::filesystem;
using namespace lsst::qserv::replica;

namespace {

string const description =
        "This is an  application which acts as a a catalog data loading"
        " client of the Replication system's catalog data ingest server.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = false;

string parse(string const& context, json const& jsonObj, string const& key) {
    if ((0 == jsonObj.count(key) or not jsonObj[key].is_string())) {
        throw invalid_argument(context + "No key for <" + key +
                               "> found in the current element of the JSON array"
                               " or its value is not a string");
    }
    return jsonObj[key];
}

template <typename T>
T parse(string const& context, json const& jsonObj, string const& key, T minValue) {
    if ((0 == jsonObj.count(key) or not jsonObj[key].is_number())) {
        throw invalid_argument(context + "No key for <" + key +
                               "> found in the current element of the JSON array"
                               " or its value is not a number");
    }
    uint64_t const num = jsonObj[key];
    if (num < minValue or num > numeric_limits<T>::max()) {
        throw invalid_argument(context + "Failed to parse JSON object, a value " + to_string(num) + " of <" +
                               key + "> is not in a range of " + to_string(minValue) + "-" +
                               to_string(numeric_limits<T>::max()) + ".");
    }
    return (T)num;
}

}  // namespace

namespace lsst::qserv::replica {

list<FileIngestApp::FileIngestSpec> FileIngestApp::parseFileList(json const& jsonObj, bool shortFormat,
                                                                 TransactionId transactionId,
                                                                 std::string const& tableName,
                                                                 std::string const& tableType) {
    string const context = "FileIngestApp::" + string(__func__) + "  ";

    list<FileIngestApp::FileIngestSpec> files;

    if (not jsonObj.is_array()) {
        throw invalid_argument(context +
                               "The input parameter doesn't represent a JSON array of file"
                               " specifications.");
    }
    if (shortFormat) {
        if (tableName.empty()) {
            throw invalid_argument(context + "The name of the table can't be empty");
        }
        if ((tableType != "R") and (tableType != "P")) {
            throw invalid_argument(context + "The value '" + tableType +
                                   "' of the table type is not in a set of {'R','P'}.");
        }
    }
    for (auto&& fileSpecJson : jsonObj) {
        if (not fileSpecJson.is_object()) {
            throw invalid_argument(context +
                                   "The next element in the JSON array doesn't represent a JSON object"
                                   " with a file specification.");
        }
        FileIngestApp::FileIngestSpec file;
        file.workerHost = parse(context, fileSpecJson, "worker-host");
        file.workerPort = parse<uint16_t>(context, fileSpecJson, "worker-port", 1);
        if (shortFormat) {
            file.transactionId = transactionId;
            file.tableName = tableName;
            file.tableType = tableType;
        } else {
            file.transactionId = parse<TransactionId>(context, fileSpecJson, "transaction-id", 0);
            file.tableName = parse(context, fileSpecJson, "table");

            string tableType = parse(context, fileSpecJson, "type");
            transform(tableType.begin(), tableType.end(), tableType.begin(), ::toupper);
            if ((tableType != "R") and (tableType != "P")) {
                throw invalid_argument(+"Failed to parse JSON object, a value " + tableType +
                                       " of <type> is not in a set of {'R','P'}.");
            }
            file.tableType = tableType;
        }
        file.inFileName = parse(context, fileSpecJson, "path");
        files.push_back(file);
    }
    return files;
}

FileIngestApp::ChunkContribution FileIngestApp::parseChunkContribution(string const& filename) {
    regex const re("^chunk_([0-9]+)(_overlap)?\\.txt$", regex::extended);
    smatch match;
    if (not regex_search(filename, match, re) or match.size() != 3) {
        throw invalid_argument("FileIngestApp::" + string(__func__) +
                               "allowed file names for contributions into partitioned tables:"
                               " 'chunk_<chunk>.txt', 'chunk_<chunk>_overlap.txt'");
    }
    ChunkContribution result;
    result.chunk = stoul(match[1].str());
    result.isOverlap = not match[2].str().empty();
    return result;
}

FileIngestApp::Ptr FileIngestApp::create(int argc, char* argv[]) {
    return Ptr(new FileIngestApp(argc, argv));
}

FileIngestApp::FileIngestApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    parser().commands("command", {"PARSE", "FILE", "FILE-LIST", "FILE-LIST-TRANS"}, _command)
            .option("fields-terminated-by", "An optional character which separates fields within a row.",
                    _dialectInput.fieldsTerminatedBy)
            .option("fields-enclosed-by", "An optional character which is used to quote fields within a row.",
                    _dialectInput.fieldsEnclosedBy)
            .option("fields-escaped-by",
                    "An optional character which is used to escape special characters (reserved by MySQL)"
                    " within a row",
                    _dialectInput.fieldsEscapedBy)
            .option("lines-terminated-by", "An optional character which is used to terminate lines.",
                    _dialectInput.linesTerminatedBy)
            .option("max-num-warnings",
                    "The number of MySQL warnings to be captured and retained"
                    " after loading the contribution. The default value of 0 will result in using"
                    " the corresponding default limit of the ingest service.",
                    _maxNumWarnings)
            .option("record-size-bytes",
                    "An optional parameter specifying the record size for reading from the input"
                    " file and for sending data to a server.",
                    _recordSizeBytes)
            .option("charset-name",
                    "An optional parameter specifying the desired name of a character set to be"
                    " used when ingesting the contribution into the destination table. If no"
                    " specific name is provided then the name at the current configuration"
                    " of the ingest service will be assumed.",
                    _charsetName)
            .flag("verbose", "Print various stats upon a completion of the ingest", _verbose);

    parser().command("PARSE")
            .description(
                    "Parse the 'infile' to locate rows according to the specified field terminator,"
                    " field quotation, escape and line terminator strings. Print each row onto"
                    " 'outfile'. The row will be preceeded by the row number.")
            .required("infile", "A path to an input file to be parsed.", _inFileName)
            .required("outfile", "A path to the output file to write the result.", _outFileName);

    parser().command("FILE")
            .description(
                    "The single file ingest option. A destination of the ingest and a path to"
                    " the file to ingest are specified via a group of mandatory parameters.")
            .required("worker-host", "The name of a worker host the Ingest service is run.", _file.workerHost)
            .required("worker-port", "The port number of the worker's Ingest service.", _file.workerPort)
            .required("transaction-id",
                      "A unique identifier (number) of a super-transaction which must be already"
                      " open.",
                      _file.transactionId)
            .required("table", "The name of a table to be ingested.", _file.tableName)
            .required("type",
                      "The type of a table to be ingested. Allowed options: 'P' for contributions"
                      " into partitioned tables, and 'R' for contributions into the regular tables.",
                      _file.tableType)
            .required("infile", "A path to an input file to be sent to the worker.", _file.inFileName);

    parser().command("FILE-LIST")
            .description(
                    "The batch ingest option. A list of files to be ingested will be read from"
                    " a file. The content of the file is required to be a serialized JSON array"
                    " of objects. Each object specifies a destination of the ingest and"
                    " the name name of a file to ingest. The general schema of the JSON object is:"
                    " [{\"worker-host\":<string>,\"worker-port\":<number>,\"transaction-id\":<number>,"
                    " \"table\":<string>,\"type\":<string>,\"path\":<string>},...]."
                    " Where allowed values for the key \"type\" are either \"P\" for"
                    " the partitioned (chunked) table contributions, or \"R\" for the"
                    " regular tables contributions. Input files for the partitioned tables"
                    " are expected to have the following names: \"chunk_<num>.txt\" or"
                    " \"chunk_<num>_overlap.txt\". The files will be ingested sequentially.")
            .required("file-list",
                      "The name of a file with ingest specifications. If the file name is set to '-'"
                      " then the specifications will be read from the standard input stream",
                      _fileListName);

    parser().command("FILE-LIST-TRANS")
            .description(
                    "The alternative batch ingest option. A list of files to be ingested will be read"
                    " from a file. The content of the file is required to be a serialized JSON array"
                    " of objects. Each object specifies a destination of the ingest and"
                    " the name name of a file to ingest. The general schema of the JSON object is:"
                    " [{\"worker-host\":<string>,\"worker-port\":<number>,\"path\":<string>},...]."
                    " Input files for the partitioned tables are expected to have the following"
                    " names: \"chunk_<num>.txt\" or \"chunk_<num>_overlap.txt\". The files will be"
                    " ingested sequentially.")
            .required("transaction-id",
                      "A unique identifier (number) of a super-transaction which must be already"
                      " open.",
                      _file.transactionId)
            .required("table", "The name of a table to be ingested.", _file.tableName)
            .required("type",
                      "The type of a table to be ingested. Allowed options: 'P' for contributions"
                      " into partitioned tables, and 'R' for contributions into the regular tables.",
                      _file.tableType)
            .required("file-list",
                      "The name of a file with ingest specifications. If the file name is set to '-'"
                      " then the specifications will be read from the standard input stream",
                      _fileListName);
}

int FileIngestApp::runImpl() {
    string const context = "FileIngestApp::" + string(__func__) + "  ";

    if (_command == "PARSE") {
        _parseFile();
        return 0;
    }
    list<FileIngestSpec> files;
    if (_command == "FILE") {
        files.push_back(_file);
    } else if (_command == "FILE-LIST") {
        bool const shortFormat = false;
        files = _readFileList(shortFormat);
    } else if (_command == "FILE-LIST-TRANS") {
        bool const shortFormat = true;
        files = _readFileList(shortFormat);
    } else {
        throw invalid_argument(context + "Unsupported loading method " + _command);
    }
    for (auto&& file : files) {
        _ingest(file);
    }
    return 0;
}

void FileIngestApp::_parseFile() const {
    string const context = "FileIngestApp::" + string(__func__) + "  ";
    ifstream infile(_inFileName, ios::binary);
    if (!infile.good()) {
        throw invalid_argument(context + "Failed to open file: '" + _inFileName + "'.");
    }
    ofstream outfile(_outFileName, ios::trunc | ios::binary);
    if (!outfile.good()) {
        throw invalid_argument(context + "Failed to create file: '" + _outFileName + "'.");
    }
    csv::Dialect dialect(_dialectInput);
    csv::Parser parser(dialect);
    size_t inNumBytes = 0;
    size_t outNumBytes = 0;
    size_t numLines = 0;
    unique_ptr<char[]> const record(new char[_recordSizeBytes]);
    bool eof = false;
    do {
        eof = !infile.read(record.get(), _recordSizeBytes);
        if (eof && !infile.eof()) {
            runtime_error(context + "Failed to read the file '" + _inFileName + "', error: '" +
                          strerror(errno) + "', errno: " + to_string(errno) + ".");
        }
        size_t const num = infile.gcount();
        inNumBytes += num;
        // Flush on the end of file
        parser.parse(record.get(), num, eof, [&](char const* buf, size_t size) {
            if (!outfile.write(buf, size)) {
                runtime_error(context + "Failed to write into the file '" + _outFileName + "', error: '" +
                              strerror(errno) + "', errno: " + to_string(errno) + ".");
            }
            outNumBytes += size;
            numLines++;
        });
    } while (!eof);
    cout << "read: " << inNumBytes << " bytes, "
         << "wrote: " << outNumBytes << " bytes, "
         << "lines: " << numLines << endl;
}

list<FileIngestApp::FileIngestSpec> FileIngestApp::_readFileList(bool shortFormat) const {
    string const context = "FileIngestApp::" + string(__func__) + "  ";

    ifstream file(_fileListName);
    if (not file.good()) {
        throw invalid_argument(context + "Failed to open file: " + _fileListName);
    }
    string str;
    try {
        str = string(istreambuf_iterator<char>(file), istreambuf_iterator<char>());
    } catch (exception const& ex) {
        throw invalid_argument(context + "Failed to read file: " + _fileListName +
                               ", exception: " + string(ex.what()));
    }
    json jsonObj;
    try {
        jsonObj = json::parse(str);
    } catch (exception const& ex) {
        throw invalid_argument(context + "Failed to parse the content of file: " + _fileListName +
                               " into a JSON object, exception: " + string(ex.what()));
    }
    return parseFileList(jsonObj, shortFormat, _file.transactionId, _file.tableName, _file.tableType);
}

void FileIngestApp::_ingest(FileIngestSpec const& file) const {
    string const context = "FileIngestApp::" + string(__func__) + "  ";

    if (file.inFileName.empty()) {
        throw invalid_argument(context + "the filename is empty");
    }
    std::error_code ec;
    fs::file_status const status = fs::status(fs::path(file.inFileName), ec);
    if (status.type() == fs::file_type::none) {
        throw runtime_error(context + "failed to check status of file: " + file.inFileName +
                            ", code: " + to_string(ec.value()) + ", error: " + ec.message());
    }
    if (!fs::is_regular_file(status)) {
        throw runtime_error(context + "not a regular file: " + file.inFileName);
    }

    // For partitioned tables analyze file name and extract a chunk number and
    // the 'overlap' attribute
    ChunkContribution chunkContribution;
    if (file.tableType == "P") {
        // Remove a base path (if any) from the file name before parsing the name
        chunkContribution =
                parseChunkContribution(fs::absolute(fs::path(file.inFileName)).filename().string());
    } else if (file.tableType == "R") {
        // No special requirements for the names of the regular files
        ;
    } else {
        throw invalid_argument(context + "a value '" + file.tableType +
                               "' of <type> is not in a set of {P,R}.");
    }

    // Push the file
    //
    // TODO: consider doing this asynchronously in a separate thread while
    // limiting the maximum duration of the operation by a timeout (BOOST ASIO
    // timer launched in a separate thread). A duration of the timeout could be
    // set via an optional parameter to the application.

    uint64_t const startedMs = util::TimeUtils::now();
    auto const ptr = IngestClient::connect(
            file.workerHost, file.workerPort, file.transactionId, file.tableName, chunkContribution.chunk,
            chunkContribution.isOverlap, file.inFileName, httpAuthContext().authKey, _dialectInput,
            _charsetName, _maxNumWarnings, _recordSizeBytes);
    ptr->send();
    uint64_t const finishedMs = util::TimeUtils::now();

    if (_verbose) {
        uint64_t const elapsedMs = max(1UL, finishedMs - startedMs);
        double const elapsedSec = elapsedMs / 1000;
        double const megaBytesPerSec = ptr->sizeBytes() / 1000000 / elapsedSec;
        cout << "                     Id: " << ptr->id() << "\n"
             << "Ingest service location: " << file.workerHost << ":" << file.workerPort << "\n"
             << " Transaction identifier: " << file.transactionId << "\n"
             << "      Destination table: " << file.tableName << "\n"
             << "                  Chunk: " << chunkContribution.chunk << "\n"
             << "       Is chunk overlap: " << bool2str(chunkContribution.isOverlap) << "\n"
             << "        Input file name: " << file.inFileName << "\n"
             << "            Start  time: "
             << util::TimeUtils::toDateTimeString(chrono::milliseconds(startedMs)) << "\n"
             << "            Finish time: "
             << util::TimeUtils::toDateTimeString(chrono::milliseconds(finishedMs)) << "\n"
             << "           Elapsed time: " << elapsedSec << " sec\n"
             << "             Bytes sent: " << ptr->sizeBytes() << "\n"
             << "              MByte/sec: " << megaBytesPerSec << "\n"
             << "     Number of warnings: " << ptr->numWarnings() << "\n"
             << "  Number of rows parsed: " << ptr->numRows() << "\n"
             << "  Number of rows loaded: " << ptr->numRowsLoaded() << "\n"
             << endl;
    }
}

}  // namespace lsst::qserv::replica
