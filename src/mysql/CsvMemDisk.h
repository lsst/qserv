// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_MYSQL_CSVMEMDISK_H
#define LSST_QSERV_MYSQL_CSVMEMDISK_H

// System headers
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <fstream>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

// Third-party headers
#include <mysql/mysql.h>

// qserv headers
#include "global/intTypes.h"
#include "mysql/CsvBuffer.h"

namespace lsst::qserv::mysql {

/// Track how much space is needed to store the current UberJob results while
/// transferring them from the workers and merging them to the result table.
/// How this effects the process depends on the TransferMethod. RAII methods
/// are used to ensure all allocations are freed.
/// MEMORYDISK - Instead new transfers waiting for memory to be
///      freed, most of the data will be written to disk when `_max` is
///      reached. The current plan is, per UberJob, to write create a
///      few CsvBuffers as is done now, and then write everything to
///      disk, and have pop read off disk when it runs out of existing
///      CsvBuffers. UberJobs with reasonable result sizes should be
///      unaffected.
/// @see CsvStrMemDisk
class TransferTracker {
public:
    using Ptr = std::shared_ptr<TransferTracker>;

    TransferTracker() = delete;

    static bool verifyDir(std::string const& dirName);
    static std::string getBaseFileName() { return std::string("qservtransfer"); }

    /// This class makes certain that any memory added to MemoryTracker
    /// is removed from MemoryTracker.
    class MemoryRaii {
    public:
        using Ptr = std::shared_ptr<MemoryRaii>;
        MemoryRaii() = delete;
        ~MemoryRaii() { _globalMt->_decrTotal(memSize); }

        size_t const memSize;
        friend class TransferTracker;

    private:
        /// Only to be called by createRaii(), which locks the mutex.
        explicit MemoryRaii(size_t memSize_) : memSize(memSize_) { _globalMt->_incrTotal(memSize); }
    };
    friend class MemoryRaii;

    static void setup(std::size_t max, std::string const& directory, std::size_t minBytesInMem,
                      std::size_t maxResultTableSizeBytes, CzarIdType czarId);
    static Ptr get() { return _globalMt; }

    /// Create a MemoryRaii instance to track `fileSize` bytes, and wait for free memory if `wait` is true.
    MemoryRaii::Ptr createRaii(size_t fileSize);

    size_t getTotal() const {
        std::lock_guard lg(_mtx);
        return _total;
    }

    std::size_t getMax() const { return _max; }
    std::string getDirectory() const { return _directory; }
    std::size_t getMinBytesInMem() const { return _minBytesInMem; }
    CzarIdType getCzarId() const { return _czarId; }

private:
    TransferTracker(std::size_t max, std::string const& directory, std::size_t minBytesInMem,
                    CzarIdType czarId)
            : _max(max), _directory(directory), _minBytesInMem(minBytesInMem), _czarId(czarId) {}

    /// This function only to be called via createRaii.
    void _incrTotal(size_t sz);

    /// This function only to be called by ~MemoryRaii()
    void _decrTotal(size_t sz);

    static Ptr _globalMt;

    mutable std::mutex _mtx;
    std::size_t _total = 0;
    std::size_t const _max;
    std::string const _directory;
    std::size_t const _minBytesInMem;
    CzarIdType const _czarId;
};

/// Store transfer data in memory until too much memory is being used.
/// By setting the maximum acceptable amount of memory to 0, this
/// effectively becomes writing results to disk.
/// Collecting data from the worker, writing it to disk, reading
/// it back, and merging is expected to be linear, run within a
/// single thread.
/// The intention is that most reasonable size requests can be handled
/// within memory, which is highly likely to be the fastest method.
/// If a lot of memory (more than TransferTraker::_max) is being used by
/// all current transfers, then transfers greater than _minBytesInMem
/// will be written to disk until memory is free.
/// If _contaminated or _fileError get set to true, there are probably
/// catastrophic file system problems.
class CsvMemDisk {
public:
    enum FileState { INIT, OPEN_W, CLOSE_W, OPEN_R, CLOSED };

    static std::shared_ptr<CsvMemDisk> create(std::size_t expectedBytes, QueryId qId, UberJobId ujId) {
        return std::shared_ptr<CsvMemDisk>(new CsvMemDisk(expectedBytes, qId, ujId));
    }

    CsvMemDisk() = delete;
    CsvMemDisk(CsvMemDisk const&) = delete;
    CsvMemDisk& operator=(CsvMemDisk const&) = delete;
    ~CsvMemDisk();

    void push(char const* data, std::size_t size);

    std::shared_ptr<std::string> pop();

    /// This version never waits.
    void transferDataFromWorker(std::function<void()> transferFunc);

    /// True if a file error happened before results would be contaminated.
    bool isFileError() const { return _fileError; }

    /// Stop transferring data before if the query has been cancelled.
    void cancel() { _cancelled = true; }

    /// Indicates there was a file system error and the file could not be opened.
    bool getContaminated() const { return _contaminated; }

    void increaseBytesFetched(size_t bytesToCopy) { _bytesFetched += bytesToCopy; }
    size_t getBytesFetched() const { return _bytesFetched; }

private:
    CsvMemDisk(std::size_t expectedBytes, QueryId qId, UberJobId ujId);

    void _writeToTmpfile(char const* data, std::size_t size);

    /// Read from the file, which should only happen after all writing has finished.
    std::shared_ptr<std::string> _readFromTmpFile();

    bool _mustWriteToTmpFile();

    void _setContaminated() { _contaminated = true; }

    std::atomic<bool> _cancelled = false;
    size_t _bytesFetched = 0;
    std::list<std::shared_ptr<std::string>> _records;

    size_t _bytesRead = 0;
    size_t const _expectedBytes;

    /// Indicates there was a file system error and the file could not be opened.
    bool _contaminated = false;

    /// Have at least on record ready to be pushed
    unsigned int const _minRecordsSize = 1;
    std::size_t _minBytesInMem;

    bool _writingToTmpFile = false;
    std::string const _directory;
    std::string const _baseName;
    QueryId const _qId;
    UberJobId const _ujId;

    std::atomic<FileState> _fState = INIT;
    std::string _filePath;  ///< file path, constant once set.
    std::fstream _file;

    bool _fileError = false;
    std::size_t _bytesWrittenToTmp = 0;
    std::size_t _bytesLeft = 0;

    TransferTracker::MemoryRaii::Ptr _memRaii;
};

std::shared_ptr<CsvBuffer> newCsvMemDiskBuffer(std::shared_ptr<CsvMemDisk> const& csvMemDisk);

}  // namespace lsst::qserv::mysql
#endif  // LSST_QSERV_MYSQL_CSVMEMDISK_H
