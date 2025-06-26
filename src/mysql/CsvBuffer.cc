// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#include "mysql/CsvBuffer.h"

// System headers
#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdio>
#include <stdexcept>
#include <string.h>

// Third-party headers
#include <mysql/mysql.h>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/LocalInfileError.h"
#include "mysql/MySqlUtils.h"
#include "util/Bug.h"

using namespace std;
namespace sfs = std::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.mysql.CsvBuffer");

string const mysqlNull("\\N");
int const largeRowThreshold = 500 * 1024;  // should be less than 0.5 * infileBufferSize

}  // namespace

namespace lsst::qserv::mysql {

/// Row is a mysql row abstraction that bundles field sizes and counts. Row is
/// shallow, and does not perform any memory management.
struct Row {
    Row() : row(nullptr), lengths(nullptr), numFields(-1) {}

    // Shallow copies all-around.
    Row(char** row_, unsigned long int* lengths_, int numFields_)
            : row(row_), lengths(lengths_), numFields(numFields_) {}

    unsigned int minRowSize() const {
        unsigned int sum = 0;
        for (int i = 0; i < numFields; ++i) {
            sum += lengths[i];
        }
        return sum;
    }

    char** row;
    unsigned long int* lengths;
    int numFields;
};

inline unsigned updateEstRowSize(unsigned lastRowSize, Row const& r) {
    unsigned const rowSize = r.minRowSize();
    return lastRowSize < rowSize ? rowSize : lastRowSize;
}

inline int addString(char* cursor, string const& s) {
    int const sSize = s.size();
    memcpy(cursor, s.data(), sSize);
    return sSize;
}

inline int maxColFootprint(int columnLength, string const& sep) {
    const int overhead = 2 + sep.size();  // NULL decl + sep size
    return overhead + (2 * columnLength);
}

inline int addColumn(char* cursor, char* colData, int colSize) {
    int added = 0;
    if (colData) {
        // Sanitize field.
        // Don't need mysql_real_escape_string, because we can
        // use the simple LOAD DATA INFILE escaping rules
        added = mysql::escapeString(cursor, colData, colSize);
    } else {
        added = addString(cursor, ::mysqlNull);
    }
    return added;
}

class ResCsvBuffer : public CsvBuffer {
public:
    ResCsvBuffer(MYSQL_RES* result);
    unsigned fetch(char* buffer, unsigned bufLen) override;
    unsigned int _addRow(Row r, char* cursor, int remaining);
    bool _fetchRow(Row& r);
    unsigned _fetchFromLargeRow(char* buffer, int bufLen);
    void _initializeLargeRow(Row const& largeRow);
    string dump() const override;

private:
    MYSQL_RES* _result;
    bool _useLargeRow;
    int _numFields;

    // Large-row support
    Row _largeRow;
    int _fieldOffset;

    string _sep;
    string _rowSep;
};

ResCsvBuffer::ResCsvBuffer(MYSQL_RES* result)
        : _result(result), _useLargeRow(false), _fieldOffset(0), _sep("\t"), _rowSep("\n") {
    // Defer actual row fetching until fetch() is called
    assert(result);
    _numFields = mysql_num_fields(result);
    // cout << _numFields << " fields per row\n";
}

string ResCsvBuffer::dump() const {
    string str = string("ResCsvBuffer _numFields=") + to_string(_numFields);
    return str;
}

unsigned ResCsvBuffer::fetch(char* buffer, unsigned bufLen) {
    unsigned fetchSize = 0;
    unsigned estRowSize = 0;
    if (bufLen <= 0) {
        throw LocalInfileError("ResCsvBuffer::fetch Can't fetch non-positive bytes");
    }
    if (_useLargeRow) {
        return _fetchFromLargeRow(buffer, bufLen);
    }
    // Loop for full rows until buffer is full, or we've detected
    // a large row.
    while ((2 * estRowSize) > (bufLen - fetchSize)) {
        // Try to fetch to fill the buffer.
        Row r;
        bool fetchOk = _fetchRow(r);
        if (!fetchOk) {
            return fetchSize;
        }
        estRowSize = updateEstRowSize(estRowSize, r);
        if (estRowSize > static_cast<unsigned>(::largeRowThreshold)) {
            _initializeLargeRow(r);
            unsigned largeFetchSize = _fetchFromLargeRow(buffer + fetchSize, bufLen - fetchSize);
            return fetchSize + largeFetchSize;
        } else {  // Small rows, use simpler row-at-a-time logic
            unsigned rowFetch = _addRow(r, buffer + fetchSize, bufLen - fetchSize);
            if (!rowFetch) {
                break;
            }
            fetchSize += rowFetch;
            fetchSize += addString(buffer + fetchSize, _rowSep);
        }
    }
    return fetchSize;
}

/// Add a row to the buffer pointed to by cursor.
/// @return the number of bytes added.
unsigned int ResCsvBuffer::_addRow(Row r, char* cursor, int remaining) {
    assert(remaining >= 0);  // negative remaining is nonsensical
    char* original = cursor;
    unsigned sepSize = _sep.size();
    // 2x orig size to allow escaping + separators +
    // null-terminator for mysql_real_escape_string
    unsigned allocRowSize = (2 * r.minRowSize()) + ((r.numFields - 1) * sepSize) + 1;
    if (allocRowSize > static_cast<unsigned>(remaining)) {
        // Make buffer size in LocalInfile larger than largest row.
        // largeRowThreshold should prevent this.
        throw LocalInfileError("ResCsvBuffer::_addRow: Buffer too small for row");
    }
    for (int i = 0; i < r.numFields; ++i) {
        if (i) {  // add separator
            cursor += addString(cursor, _sep);
        }
        cursor += addColumn(cursor, r.row[i], r.lengths[i]);
    }
    assert(cursor > original);
    return cursor - original;
}

/// Fetch a row from _result and fill the caller-supplied Row.
bool ResCsvBuffer::_fetchRow(Row& r) {
    MYSQL_ROW mysqlRow = mysql_fetch_row(_result);
    if (!mysqlRow) {
        return false;
    }
    r.row = mysqlRow;
    r.lengths = mysql_fetch_lengths(_result);
    r.numFields = _numFields;
    assert(r.lengths);
    return true;
}

/// Attempt to fill a buffer from a large row that may not completely fit in
/// the buffer.
/// This is unfinished code, but is only triggered for rows > 500kB.  Also,
/// CsvBuffer is an interface for accessing the row data for LocalInfile, and because
/// ResCsvBuffer is an implementation that fetches rows from a MYSQL_RES handle,
/// and Qserv will generally use rows received from workers as CSV-formatted
/// files, ResCsvBuffer objects are not planned for use in a normally
/// operating Qserv system. Still, ResCsvBuffer is useful for *testing*
/// LocalInfile (e.g., loading the result of a SELECT statement using LOAD DATA
/// INFILE).
unsigned ResCsvBuffer::_fetchFromLargeRow(char* buffer, int bufLen) {
    // Insert field-at-a-time,
    char* cursor = buffer;
    int remaining = bufLen;

    while (maxColFootprint(_largeRow.lengths[_fieldOffset], _sep) > remaining) {
        int addLength = addColumn(cursor, _largeRow.row[_fieldOffset], _largeRow.lengths[_fieldOffset]);
        cursor += addLength;
        remaining -= addLength;
        ++_fieldOffset;
        if (_fieldOffset >= _numFields) {
            if (!_fetchRow(_largeRow)) {
                break;
                // no more rows, return what we have
            }
            _fieldOffset = 0;
        }
        // FIXME: unfinished
    }
    if (cursor == buffer) {  // Were we able to put anything in?
        throw LocalInfileError("ResCsvBuffer::_fetchFromLargeRow: Buffer too small for single column!");
    }
    return bufLen - remaining;
}

/// Init structures for large rows.
void ResCsvBuffer::_initializeLargeRow(Row const& largeRow) {
    _useLargeRow = true;
    _fetchRow(_largeRow);
    _fieldOffset = 0;
}

shared_ptr<CsvBuffer> newResCsvBuffer(MYSQL_RES* result) { return make_shared<ResCsvBuffer>(result); }

CsvStream::CsvStream(size_t maxRecords) : _maxRecords(maxRecords) {
    if (maxRecords == 0) {
        throw invalid_argument("CsvStream::CsvStream: maxRecords must be greater than 0");
    }
}

void CsvStream::cancel() {
    unique_lock<mutex> lock(_mtx);
    _cancelled = true;
    _cv.notify_all();
}

void CsvStream::push(char const* data, size_t size) {
    unique_lock<mutex> lock(_mtx);
    _cv.wait(lock, [this]() { return (_records.size() < _maxRecords) || _cancelled; });

    if (_cancelled) return;
    if (data != nullptr && size != 0) {
        _records.emplace_back(make_shared<string>(data, size));
    } else {
        // Empty string is meant to indicate the end of the stream.
        _records.emplace_back(make_shared<string>());
    }
    _cv.notify_one();
}

shared_ptr<string> CsvStream::pop() {
    unique_lock<mutex> lock(_mtx);
    _cv.wait(lock, [this]() { return (!_records.empty() || _cancelled); });

    if (_records.empty()) {
        // _cancelled must be true.
        // The hope is that this never happens, but to keep the system
        // from locking up, send out illegal characters to force fail
        // the merge. Need to keep sending characters until the
        // database stops asking for them.
        // See CsvStream::cancel()
        _contaminated = true;
        auto pstr = make_shared<string>("$");
        _cv.notify_one();
        return pstr;
    }
    shared_ptr<string> front = _records.front();
    _records.pop_front();
    _cv.notify_one();
    return front;
}

bool CsvStream::empty() const {
    unique_lock<mutex> lock(_mtx);
    return _records.empty();
}

void CsvStream::waitReadyToRead() {
    // No need to wait for this class
    thread thrd(_csvLambda);
    _thrd = move(thrd);
    _thrdStarted = true;
}

/**
 * CsvStreamBuffer is a CsvBuffer that reads from a CsvStream. It is used to read
 * data from a CsvStream in a buffered manner.
 * @note The current implementation of method fetch() could be further optimized
 * to fetch more than one record at a time. The current implementation
 * fetches one record at a time, which may be inefficient for small records.
 * Though, in practice, this is not an issue in the current design of the result
 * merging algorithm.
 */
class CsvStreamBuffer : public CsvBuffer {
public:
    explicit CsvStreamBuffer(shared_ptr<CsvStream> const& csvStream) : _csvStream(csvStream) {}

    ~CsvStreamBuffer() override = default;

    unsigned fetch(char* buffer, unsigned bufLen) override {
        if (bufLen == 0) {
            throw LocalInfileError("CsvStreamBuffer::fetch Can't fetch non-positive bytes");
        }
        auto csvStrm = _csvStream.lock();
        if (csvStrm == nullptr) return 0;
        if (_str == nullptr) {
            _str = csvStrm->pop();
            _offset = 0;
        }
        if (_str->empty()) return 0;
        if (_offset >= _str->size()) {
            _str = csvStrm->pop();
            _offset = 0;
            if (_str->empty()) return 0;
        }
        unsigned const bytesToCopy = min(bufLen, static_cast<unsigned>(_str->size() - _offset));
        ::memcpy(buffer, _str->data() + _offset, bytesToCopy);
        _offset += bytesToCopy;
        csvStrm->increaseBytesWrittenBy(bytesToCopy);
        return bytesToCopy;
    }

    string dump() const override { return "CsvStreamBuffer"; }

private:
    weak_ptr<CsvStream> _csvStream;
    shared_ptr<string> _str;
    size_t _offset = 0;
};

shared_ptr<CsvBuffer> newCsvStreamBuffer(shared_ptr<CsvStream> const& csvStream) {
    return make_shared<CsvStreamBuffer>(csvStream);
}

TransferTracker::Ptr TransferTracker::_globalMt;

void TransferTracker::setup(string const& transferMethodStr, std::size_t max, string const& directory,
                            std::size_t minMBInMem, std::size_t maxResultTableSizeBytes) {
    if (_globalMt != nullptr) {
        throw util::Bug(ERR_LOC, "MemoryTracker::setup called when MemoryTracker already setup!");
    }
    TransferMethod tm = transferMethodFromString(transferMethodStr);
    if (tm == MEMDISK) {
        if (!verifyDir(directory)) {
            throw util::Bug(ERR_LOC, "MemoryTracker::setup called with bad directory! " + directory);
        }
    }
    size_t const MB_SIZE_BYTES = 1024 * 1024;
    if (tm == MEMORY && max < maxResultTableSizeBytes) {
        throw util::Bug(ERR_LOC,
                        "Configuration error resultdb maxTransferMemMB=" + to_string(max / MB_SIZE_BYTES) +
                                " must be larger than maxtablesize_mb= " +
                                to_string(maxResultTableSizeBytes / MB_SIZE_BYTES));
    }
    _globalMt = TransferTracker::Ptr(new TransferTracker(tm, max, directory, minMBInMem));
}

bool TransferTracker::verifyDir(string const& dirName) {
    sfs::path dir = dirName;
    if (!(sfs::exists(dir) && sfs::is_directory(dir))) {
        LOGS(_log, LOG_LVL_ERROR, "verifyDir, " + dirName + " is not a valid directory");
        return false;
    }
    return true;
}

TransferTracker::TransferMethod TransferTracker::transferMethodFromString(string const& strType) {
    string str;
    for (unsigned char c : strType) {
        str += tolower(c);
    }
    TransferMethod tMethod;
    if (str == "memory") {
        tMethod = MEMORY;
        LOGS(_log, LOG_LVL_INFO, "Result TransferMethod set to memory");
    } else if (str == "stream") {
        tMethod = STREAM;
        LOGS(_log, LOG_LVL_INFO, "Result TransferMethod set to stream");
    } else if (str == "memdisk") {
        tMethod = MEMDISK;
        LOGS(_log, LOG_LVL_INFO, "Result TransferMethod set to memdisk");
    } else {
        tMethod = MEMORY;
        LOGS(_log, LOG_LVL_ERROR,
             "Result TransferMethod set to memory due to invalid string '"
                     << strType << "'"
                     << " valid entries are 'memory', 'stream', 'memdisk'");
    }
    return tMethod;
}

TransferTracker::MemoryRaii::Ptr TransferTracker::createRaii(size_t fileSize, bool wait) {
    unique_lock ulck(_mtx);
    if (wait) {
        if (fileSize > _max) {
            throw util::Bug(ERR_LOC, "MemoryTracker::createRaii file too large " + to_string(fileSize) +
                                             " max=" + to_string(_max));
        }
        _cv.wait(ulck, [this, fileSize]() { return (fileSize + _total) < _max; });
    }
    MemoryRaii::Ptr pRaii(new MemoryRaii(fileSize));
    return pRaii;
}

void TransferTracker::_incrTotal(size_t sz) {
    // _mtx must already be locked.
    _total += sz;
    _cv.notify_one();  // Many items may be waiting on a large file, so there may be room for more
}

void TransferTracker::_decrTotal(size_t sz) {
    lock_guard ulck(_mtx);
    if (sz > _total) {
        throw util::Bug(ERR_LOC,
                        "MemoryTracker::_decrTotal sz=" + to_string(sz) + " > total=" + to_string(_total));
    }
    _total -= sz;
    _cv.notify_one();
}

void CsvStrMem::waitReadyToRead() {
    auto memTrack = TransferTracker::get();
    if (memTrack == nullptr) {
        throw util::Bug(ERR_LOC, "CsvStrMem::waitReadyToRead MemoryTracker is NULL");
    }
    bool const wait = true;
    _memRaii = memTrack->createRaii(_expectedBytes, wait);

    // Read directly without starting a separate thread.
    _csvLambda();
}

void CsvStrMem::push(char const* data, size_t size) {
    // Push is always ok, no need to wait.
    if (_cancelled) return;
    _bytesRead += size;
    if (data != nullptr && size != 0) {
        _records.emplace_back(make_shared<string>(data, size));
    } else {
        // Empty string is meant to indicate the end of the stream.
        _records.emplace_back(make_shared<string>());
    }
}

shared_ptr<string> CsvStrMem::pop() {
    shared_ptr<string> front = _records.front();
    _records.pop_front();
    return front;
}

CsvStrMemDisk::CsvStrMemDisk(std::size_t expectedBytes, QueryId qId, UberJobId ujId)
        : CsvStrMem(expectedBytes), _qId(qId), _ujId(ujId) {
    auto memTrack = TransferTracker::get();
    if (memTrack == nullptr) {
        throw util::Bug(ERR_LOC, "CsvStrMemDisk constructor MemoryTracker is NULL");
    }
    sfs::path fPath = memTrack->getDirectory();
    string fileName = memTrack->getBaseFileName() + "_" + to_string(_qId) + "_" + to_string(ujId);
    fPath /= fileName;
    _filePath = fPath;

    _minBytesInMem = memTrack->getMinBytesInMem();
}

void CsvStrMemDisk::waitReadyToRead() {
    auto memTrack = TransferTracker::get();
    if (memTrack == nullptr) {
        throw util::Bug(ERR_LOC, "CsvStrMemDisk::waitReadyToRead MemoryTracker is NULL");
    }
    bool const nowait = false;
    _memRaii = memTrack->createRaii(_expectedBytes, nowait);

    // Read directly without starting a separate thread.
    _csvLambda();
}

bool CsvStrMemDisk::_mustWriteToTmpFile() {
    // Once writing to file, this instance must keep writing to file.
    if (_writingToFile) return true;

    auto memTrack = TransferTracker::get();
    // If too much memory is being used for transfers, star writing large transfers to files.
    if (memTrack->getTotal() > memTrack->getMax()) {
        if (_records.size() > _minRecordsSize && _bytesRead > _minBytesInMem) {
            _writingToFile = true;
        }
    }
    return _writingToFile;
}

void CsvStrMemDisk::push(char const* data, size_t size) {
    // Push is always ok, no need to wait.
    if (_cancelled) return;
    _bytesRead += size;
    if (_mustWriteToTmpFile()) {
        _writeToTmpfile(data, size);
        return;
    }
    if (data != nullptr && size != 0) {
        _records.emplace_back(make_shared<string>(data, size));
    } else {
        // Empty string is meant to indicate the end of the stream.
        _records.emplace_back(make_shared<string>());
    }
}

shared_ptr<string> CsvStrMemDisk::pop() {
    if (_records.size() > 0) {
        shared_ptr<string> front = _records.front();
        _records.pop_front();
        return front;
    }
    return _readFromTmpFile();
}

void CsvStrMemDisk::_writeToTmpfile(char const* data, std::size_t size) {
    // Open the file if needed
    auto oldState = _fState.exchange(OPEN_W);
    if (oldState == INIT) {
        _file.open(_filePath, fstream::out);
    }
    if (!_file.is_open() || _fState != OPEN_W) {
        LOGS(_log, LOG_LVL_ERROR,
             "CsvStrMemDisk::_writeTofile file isn't open " << _filePath << " or bad state=" << _fState);
        _fileError = true;
        return;
    }

    _file.write(data, size);
    _bytesWrittenToTmp += size;
}

std::shared_ptr<std::string> CsvStrMemDisk::_readFromTmpFile() {
    if (_fState == OPEN_W) {
        _fState = CLOSE_W;
        _file.close();
    }
    auto oldState = _fState.exchange(OPEN_R);
    if (oldState == CLOSE_W) {
        _file.open(_filePath, fstream::in);
        _bytesLeft = _bytesWrittenToTmp;
    }
    if (!_file.is_open() || _fState != OPEN_R) {
        // This is extremely unlikely
        if (!getContaminated())
            LOGS(_log, LOG_LVL_ERROR,
                 "CsvStrMemDisk::_readFromfile file isn't open " << _filePath << " or bad state=" << _fState);
        setContaminated();
        return make_shared<string>("$");
    }

    std::size_t buffSz = std::min(10'000'000ul, _bytesLeft);
    auto strPtr = make_shared<string>();
    strPtr->resize(buffSz);
    _file.read(strPtr->data(), buffSz);
    _bytesLeft -= buffSz;
    return strPtr;
}

CsvStrMemDisk::~CsvStrMemDisk() {
    if (_fState != INIT) {
        LOGS(_log, LOG_LVL_INFO, "~CsvStrMemDisk() remove " << _filePath);
        _file.close();
        std::remove(_filePath.c_str());
    }
}

}  // namespace lsst::qserv::mysql
