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
#include "mysql/CsvMemDisk.h"

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
LOG_LOGGER _log = LOG_GET("lsst.qserv.mysql.CsvMemDisk");
}  // namespace

namespace lsst::qserv::mysql {

TransferTracker::Ptr TransferTracker::_globalMt;

void TransferTracker::setup(std::size_t max, string const& directory, std::size_t minMBInMem,
                            std::size_t maxResultTableSizeBytes) {
    if (_globalMt != nullptr) {
        throw util::Bug(ERR_LOC, "MemoryTracker::setup called when MemoryTracker already setup!");
    }
    _globalMt = TransferTracker::Ptr(new TransferTracker(max, directory, minMBInMem));
}

bool TransferTracker::verifyDir(string const& dirName) {
    sfs::path dir = dirName;
    if (!(sfs::exists(dir) && sfs::is_directory(dir))) {
        LOGS(_log, LOG_LVL_ERROR, "verifyDir, " + dirName + " is not a valid directory");
        return false;
    }
    return true;
}

TransferTracker::MemoryRaii::Ptr TransferTracker::createRaii(size_t fileSize) {
    MemoryRaii::Ptr pRaii(new MemoryRaii(fileSize));
    return pRaii;
}

void TransferTracker::_incrTotal(size_t sz) {
    lock_guard ulck(_mtx);
    _total += sz;
}

void TransferTracker::_decrTotal(size_t sz) {
    lock_guard ulck(_mtx);
    if (sz > _total) {
        throw util::Bug(ERR_LOC,
                        "MemoryTracker::_decrTotal sz=" + to_string(sz) + " > total=" + to_string(_total));
    }
    _total -= sz;
}

CsvMemDisk::CsvMemDisk(std::size_t expectedBytes, QueryId qId, UberJobId ujId)
        : _expectedBytes(expectedBytes), _qId(qId), _ujId(ujId) {
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

void CsvMemDisk::transferDataFromWorker(std::function<void()> transferFunc) {
    auto memTrack = TransferTracker::get();
    if (memTrack == nullptr) {
        throw util::Bug(ERR_LOC, "CsvStrMemDisk::waitReadyToRead MemoryTracker is NULL");
    }
    _memRaii = memTrack->createRaii(_expectedBytes);
    transferFunc();
}

bool CsvMemDisk::_mustWriteToTmpFile() {
    // Once writing to file, this instance must keep writing to file.
    if (_writingToTmpFile) return true;

    auto memTrack = TransferTracker::get();
    // If too much memory is being used for transfers, star writing large transfers to files.
    if (memTrack->getTotal() > memTrack->getMax()) {
        if (_records.size() > _minRecordsSize && _bytesRead > _minBytesInMem) {
            _writingToTmpFile = true;
        }
    }
    return _writingToTmpFile;
}

void CsvMemDisk::push(char const* data, size_t size) {
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

shared_ptr<string> CsvMemDisk::pop() {
    if (_records.size() > 0) {
        shared_ptr<string> front = _records.front();
        _records.pop_front();
        return front;
    }
    return _readFromTmpFile();
}

void CsvMemDisk::_writeToTmpfile(char const* data, std::size_t size) {
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

std::shared_ptr<std::string> CsvMemDisk::_readFromTmpFile() {
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
        // This is extremely unlikely and means something has gone wrong with the file system.
        // If something has gone wrong with the file system, a crash may be incoming.
        if (!getContaminated())
            LOGS(_log, LOG_LVL_ERROR,
                 "CsvStrMemDisk::_readFromfile file isn't open " << _filePath << " or bad state=" << _fState);
        _setContaminated();
        return make_shared<string>("$");
    }

    std::size_t buffSz = std::min(100'000ul, _bytesLeft);
    auto strPtr = make_shared<string>();
    strPtr->resize(buffSz);
    _file.read(strPtr->data(), buffSz);
    _bytesLeft -= buffSz;
    return strPtr;
}

CsvMemDisk::~CsvMemDisk() {
    if (_fState != INIT) {
        LOGS(_log, LOG_LVL_INFO, "~CsvStrMemDisk() remove " << _filePath);
        _file.close();
        std::remove(_filePath.c_str());
    }
}

class CsvMemDiskBuffer : public CsvBuffer {
public:
    explicit CsvMemDiskBuffer(shared_ptr<CsvMemDisk> const& csvMemDisk) : _csvMemDisk(csvMemDisk) {}

    ~CsvMemDiskBuffer() override = default;

    unsigned fetch(char* buffer, unsigned bufLen) override {
        if (bufLen == 0) {
            throw LocalInfileError("CsvStreamBuffer::fetch Can't fetch non-positive bytes");
        }
        auto csvStrm = _csvMemDisk.lock();
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
        csvStrm->increaseBytesFetched(bytesToCopy);
        return bytesToCopy;
    }

    string dump() const override { return "CsvStreamBuffer"; }

private:
    weak_ptr<CsvMemDisk> _csvMemDisk;
    shared_ptr<string> _str;
    size_t _offset = 0;
};

shared_ptr<CsvBuffer> newCsvMemDiskBuffer(shared_ptr<CsvMemDisk> const& csvMemDisk) {
    return make_shared<CsvMemDiskBuffer>(csvMemDisk);
}

}  // namespace lsst::qserv::mysql
