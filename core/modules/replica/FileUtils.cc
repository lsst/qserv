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
#include "replica/FileUtils.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <stdexcept>
#include <sys/types.h>
#include <pwd.h>
#include <unistd.h>

// Qserv headers
#include "replica/Configuration.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {

/// Standard file extension of the MyISAM (and derived) engine's table files
vector<string> const extensions{"frm", "MYD", "MYI"};

/**
 * Evaluate if an input string corresponds to a valid file extension
 *
 * @param str - the candidate string to be tested
 *
 * @return 'true' if this is a valid file extension
 */
bool isValidExtention(string const& str) {
    return extensions.end() != find(extensions.begin(), extensions.end(), str);
}


/**
 * Evaluate if an input string corresponds to a valid partitioned table
 * or its variant.
 *
 * @param str          - the candidate string to be tested
 * @param databaseInfo - database specification
 *
 * @return 'true' if this is a valid table name
 */
bool isValidPartitionedTable(
            string const& str,
            DatabaseInfo const& databaseInfo) {

    for (auto&& table: databaseInfo.partitionedTables) {
        if (str == table) return true;
        if (str == table + "FullOverlap") return true;
    }
    return false;
}
} // namespace

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////
//    class FileUtils    //
///////////////////////////

vector<string> FileUtils::partitionedFiles(DatabaseInfo const& databaseInfo,
                                           unsigned int chunk) {

    vector<string> result;

    string const chunkSuffix = "_" + to_string(chunk);

    for (auto&& table: databaseInfo.partitionedTables) {

        string const file = table + chunkSuffix;
        for (auto&& ext: ::extensions) {
            result.push_back(file + "." + ext);
        }
        string const fileOverlap = table + "FullOverlap" + chunkSuffix;
        for (auto&& ext: ::extensions) {
            result.push_back(fileOverlap + "." + ext);
        }
    }
    return result;
}


vector<string> FileUtils::regularFiles(DatabaseInfo const& databaseInfo) {

    vector<string> result;

    for (auto&& table : databaseInfo.regularTables) {
        string const filename = table;
        for (auto&& ext: ::extensions) {
            result.push_back(filename + "." + ext);
        }
    }
    return result;
}


bool FileUtils::parsePartitionedFile(tuple<string, unsigned int, string> &parsed,
                                     string  const& fileName,
                                     DatabaseInfo const& databaseInfo) {

    // Find the extension of the file and evaluate it if found

    string::size_type const posBeforeExention = fileName.rfind('.');
    if (posBeforeExention == string::npos) return false;               // not found

    string const extention = fileName.substr(posBeforeExention + 1);   // excluding '.'
    if (!::isValidExtention(extention)) return false;                       // unknown file extension

    // Find and parse the chunk number

    string::size_type const posBeforeChunk = fileName.rfind('_');
    if (posBeforeChunk == string::npos) return false;  // not found
    if (posBeforeChunk >= posBeforeExention) return false;  // no room for chunk

    unsigned int chunk;
    try {
        chunk = stoul(fileName.substr(posBeforeChunk + 1, posBeforeExention - posBeforeChunk - 1));
    } catch (invalid_argument const&) {
        return false;
    }

    // Find the table name and check if it's allowed for the specified database

    const string table = fileName.substr(0, posBeforeChunk);
    if (!::isValidPartitionedTable(table, databaseInfo)) return false;  // unknown table

    // Success
    parsed = make_tuple(table, chunk, extention);
    return true;
}


uint64_t FileUtils::compute_cs(string const& fileName,
                               size_t recordSizeBytes) {

    if (fileName.empty()) {
        throw invalid_argument(
                "FileUtils::" + string(__func__) +
                "  empty file name passed into the method");
    }
    if (not recordSizeBytes or (recordSizeBytes > MAX_RECORD_SIZE_BYTES)) {
        throw invalid_argument(
                "FileUtils::" + string(__func__) + "  invalid record size " +
                to_string(recordSizeBytes) + "passed into the method");
    }
    FILE* fp = fopen(fileName.c_str(), "rb");
    if (not fp) {
        throw runtime_error(
                "FileUtils::" + string(__func__) + string("  file open error: ") +
                strerror(errno) + string(", file: ") + fileName);
    }
    uint8_t *buf = new uint8_t[recordSizeBytes];

    uint64_t cs = 0;
    size_t num;
    while ((num = fread(buf, sizeof(uint8_t), recordSizeBytes, fp))) {
        for (uint8_t *ptr = buf, *end = buf + num; ptr != end; ++ptr) {
            cs += (uint64_t)(*ptr);
        }
    }
    if (ferror(fp)) {
        const string err =
             "FileUtils::" + string(__func__) + string("  file read error: ") +
            strerror(errno) + string(", file: ") + fileName;
        fclose(fp);
        delete [] buf;
        throw runtime_error(err);
    }
    fclose(fp);
    delete [] buf;

    return cs;
}


string FileUtils::getEffectiveUser() {
    return string(getpwuid(geteuid())->pw_name);
}


/////////////////////////////////////
//    class FileCsComputeEngine    //
/////////////////////////////////////

FileCsComputeEngine::FileCsComputeEngine(string const& fileName,
                                         size_t recordSizeBytes)
    :   _fileName(fileName),
        _recordSizeBytes(recordSizeBytes),
        _fp(0),
        _buf(0),
        _bytes(0),
        _cs(0) {

    if (_fileName.empty()) {
        throw invalid_argument("FileCsComputeEngine:  empty file name");
    }
    if (not _recordSizeBytes or (_recordSizeBytes > FileUtils::MAX_RECORD_SIZE_BYTES)) {
        throw invalid_argument(
                "FileCsComputeEngine:  invalid record size " + to_string(_recordSizeBytes));
    }
    _fp = fopen(_fileName.c_str(), "rb");
    if (not _fp) {
        throw runtime_error(
                string("FileCsComputeEngine:  file open error: ") + strerror(errno) +
                string(", file: ") + _fileName);
    }
    _buf = new uint8_t[_recordSizeBytes];
}


FileCsComputeEngine::~FileCsComputeEngine() {
    if (_fp)  fclose(_fp);
    if (_buf) delete [] _buf;
}


bool FileCsComputeEngine::execute() {

    if (not _fp) {
        throw logic_error(
                "FileCsComputeEngine::" + string(__func__) + "  file is already closed");
    }
    size_t const num = fread(_buf, sizeof(uint8_t), _recordSizeBytes, _fp);
    if (num) {
        _bytes += num;
        for (uint8_t *ptr = _buf, *end = _buf + num; ptr != end; ++ptr) {
            _cs += (uint64_t)(*ptr);
        }
        return false;
    }

    // I/O error?
    if (ferror(_fp)) {
        string const err =
            string("FileCsComputeEngine::") + string(__func__) +
            string(" file read error: ") + strerror(errno) + string(", file: ") +
            _fileName;

        fclose(_fp);
        _fp = nullptr;

        delete [] _buf;
        _buf = nullptr;

        throw runtime_error(err);
    }

    // EOF
    fclose(_fp);
    _fp = nullptr;

    delete [] _buf;
    _buf = nullptr;

    return true;
}


//////////////////////////////////////////
//    class MultiFileCsComputeEngine    //
//////////////////////////////////////////

MultiFileCsComputeEngine::~MultiFileCsComputeEngine() {
}


MultiFileCsComputeEngine::MultiFileCsComputeEngine(vector<string> const& fileNames,
                                                   size_t recordSizeBytes)
    :   _fileNames(fileNames),
        _recordSizeBytes(recordSizeBytes) {

    if (not recordSizeBytes or (_recordSizeBytes > FileUtils::MAX_RECORD_SIZE_BYTES)) {
        throw invalid_argument(
                "MultiFileCsComputeEngine:  invalid record size " + to_string(_recordSizeBytes));
    }

    // This will be the very first file (if any) to be processed
    _currentFileItr = _fileNames.begin();

    // Open the very first file to be read if the input collection is not empty
    if (_currentFileItr != _fileNames.end()) {
        _processed[*_currentFileItr].reset(
            new FileCsComputeEngine(*_currentFileItr, _recordSizeBytes));
    }
}


bool MultiFileCsComputeEngine::processed(string const& fileName) const {

    if (_fileNames.end() == find(_fileNames.begin(),
                                 _fileNames.end(),
                                 fileName)) {
        throw invalid_argument(
                "MultiFileCsComputeEngine::" + string(__func__) +
                " unknown file: " + fileName);
    }
    return _processed.count(fileName);
}


size_t  MultiFileCsComputeEngine::bytes(string const& fileName) const {
    if (not processed(fileName)) {
        throw logic_error(
                "MultiFileCsComputeEngine::" + string(__func__) +
                "  the file hasn't been processed: " + fileName);
    }
    return _processed.at(fileName)->bytes();
}


uint64_t MultiFileCsComputeEngine::cs(string const& fileName) const {
    if (not processed(fileName)) {
        throw logic_error(
                "MultiFileCsComputeEngine::" + string(__func__) +
                "  the file hasn't been processed: " + fileName);
    }
    return _processed.at(fileName)->cs();
}


bool MultiFileCsComputeEngine::execute() {

    // All files have been processed
    if (_fileNames.end() == _currentFileItr) return true;

    // Process possible EOF of the current or any subsequent files
    // while there is any data or until running out of files.
    while (_processed[*_currentFileItr]->execute()) {

        // Move to the next file if any. If no more files then finish.
        ++_currentFileItr;
        if (_fileNames.end() == _currentFileItr) return true;

        // Open that file and expect it to be read at the next iteration
        // of this loop
        _processed[*_currentFileItr].reset(
            new FileCsComputeEngine(*_currentFileItr, _recordSizeBytes));
    }
    return false;
}

}}} // namespace lsst::qserv::replica
