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
#ifndef LSST_QSERV_MYSQL_CSVBUFFER_H
#define LSST_QSERV_MYSQL_CSVBUFFER_H

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

namespace lsst::qserv::mysql {

/**
 * Interface CsvBuffer is an abstraction for a buffer from which arbitrarily-sized
 * buckets of bytes can be read. The buffer stores the CSV-formatted payload of
 * tab-separated-field, line-delimited-tuple sequence of tuples.
 */
class CsvBuffer {
public:
    virtual ~CsvBuffer() = default;

    /// Fetch a number of bytes into a buffer. Return the number of bytes
    /// fetched. Returning less than bufLen does NOT indicate EOF.
    virtual unsigned fetch(char* buffer, unsigned bufLen) = 0;

    /// Return a descriptive string.
    virtual std::string dump() const = 0;
};

/**
 * The factory function creates a new CsvBuffer object for the given
 * MySQL result set. The function is expected to be used in the context
 * of a LocalInfile object.
 */
std::shared_ptr<CsvBuffer> newResCsvBuffer(MYSQL_RES* result);

/**
 * Class CsvStream is to manage a stream of CSV records. The class has thread-safe
 * push and pop methods to add and retrieve records from the stream.
 * The records are stored in a list, where each element of the list contains a
 * shared pointer to a string.
 * The maximum capacity of the stream is defined by a value of the maxRecords parameter
 * passed to the constructor. The number of records must be strictly greater than 0.
 * @note An empty string returned by the method pop() indicates end of the stream.
 */
class CsvStream {
public:
    using Ptr = std::shared_ptr<CsvStream>;

    /**
     * Factory function to create a new CsvStream object.
     * @param maxRecords The maximum number of records in the stream
     * @return A shared pointer to the newly created object
     * @throw std::invalid_argument if maxRecords is 0
     */
    static std::shared_ptr<CsvStream> create(std::size_t maxRecords) {
        return std::shared_ptr<CsvStream>(new CsvStream(maxRecords));
    }

    CsvStream() = delete;
    CsvStream(CsvStream const&) = delete;
    CsvStream& operator=(CsvStream const&) = delete;
    virtual ~CsvStream() = default;

    /**
     * Push a new record to the stream. The record is a string of bytes.
     * Bytes will be copied into the newly created record.
     * The method will block if the stream is full until a record is popped.
     * The empty record (data == nullptr or size==0) should be inserted to indicate
     * stream termination.
     * @param data The record to be pushed to the stream
     * @param size The size of the record
     */
    virtual void push(char const* data, std::size_t size);

    /**
     *  Call to break push operations if the results are no longer needed.
     *  This is only meant to be used to break lingering push() calls.
     *  TODO:UJ The interleaving of result file reading and table
     *       merging makes it impossible to guarantee the result
     *       table is valid in the event that communication
     *       to a worker is lost during file transfer.
     *       @see UberJob::killUberJob()
     */
    virtual void cancel();

    /*
     * Return true if this instance has been cancelled.
     */
    bool isCancelled() const { return _cancelled; }

    /**
     * Pop a record from the stream. The method will block if the stream is empty
     * until a record is pushed.
     * @return A shared pointer to the popped record or an empty string for the end of the stream
     */
    virtual std::shared_ptr<std::string> pop();

    /**
     * Check if the stream is empty.
     * @note Be advised that this operation has a limited use if the stream is
     * still being used by multiple threads. The method is meant to be used only
     * for debugging purposes (such as unit testing).
     * @return true if the stream is empty
     */
    bool empty() const;

    void increaseBytesWrittenBy(size_t bytesToCopy) { _bytesWritten += bytesToCopy; }
    size_t getBytesWritten() const { return _bytesWritten; }

    /**
     * If this returns true, the result table has been contaminated by bad characters
     * in an effort to keep the system from hanging, and the UserQuery is done.
     */
    bool getContaminated() const { return _contaminated; }

    /// The function to run to read/push the data from the worker.
    void setLambda(std::function<void()> csvLambda) { _csvLambda = csvLambda; }

    /// In this class, no waiting, just start the read/push thread.
    virtual void waitReadyToRead();

    /// Join the thread, must be called from the same thread that called waitReadyToRead
    virtual void join() {
        if (_thrdStarted) _thrd.join();
    }

protected:
    CsvStream(std::size_t maxRecords);

    void setContaminated() { _contaminated = true; }

    std::function<void()> _csvLambda;
    bool _cancelled = false;
    std::atomic<size_t> _bytesWritten;
    std::list<std::shared_ptr<std::string>> _records;

private:
    mutable std::mutex _mtx;
    std::condition_variable _cv;
    std::size_t const _maxRecords;

    std::atomic<bool> _contaminated = false;
    std::thread _thrd;
    bool _thrdStarted = false;
};

/**
 * The factory function creates a new CsvBuffer object which will forward
 * the data pulled from the specified stream to a given buffer.
 * @param csvStream The input stream that us filled with the CSV-formatted bytestream
 * @return A shared pointer to the newly created object
 */
std::shared_ptr<CsvBuffer> newCsvStreamBuffer(std::shared_ptr<CsvStream> const& csvStream);

/// Track how much space is needed to store the current UberJob results while
/// transferring them from the workers and merging them to the result table.
/// How this effects the process depends on the TransferMethod. RAII methods
/// are used to ensure all allocations are freed.
/// STREAM is unaffected by the amount of memory used, but may contaminate
///      the final result table on worker failure, forcing the user query
///      to be cancelled. Not much of an issue with small numbers of workers.
/// MEMORY UberJob results are stored in memory until all data is retrieved
///      from the worker and then merged to the result table. This isolates
///      worker failures from contaminating the result table. The down side
///      is that The result transfer won't be started until the `_total`
///      including the new data is less than the `_max`. It is terribly
///      important that `_max` is greater than the maximum result size
///      (currently 5GB). This should be checked when calling
///      MemoryTracker::setup.
/// MEMORYDISK TODO:UJ - Instead new transfers waiting for memory to be
///      freed, most of the data will be written to disk when `_max` is
///      reached. The current plan is, per UberJob, to write create a
///      few CsvBuffers as is done now, and then write everything to
///      disk, and have pop read off disk when it runs out of existing
///      CsvBuffers. UberJobs with reasonable result sizes should be
///      unaffected.
/// TODO:UJ - Test the different options and reorganize code.
class MemoryTracker {
public:
    using Ptr = std::shared_ptr<MemoryTracker>;

    MemoryTracker() = delete;

    enum TransferMethod { STREAM, MEMORY, MEMDISK };
    static TransferMethod transferMethodFromString(std::string const& str);
    static bool verifyDir(std::string const& dirName);
    static std::string getBaseFileName() { return std::string("qservtransfer"); }

    /// Return the TransferMethod.
    /// @see MemoryTracker
    TransferMethod getTransferMethod() const { return _transferMethod; }

    /// This class makes certain that any memory added to MemoryTracker
    /// is removed from MemoryTracker.
    class MemoryRaii {
    public:
        using Ptr = std::shared_ptr<MemoryRaii>;
        MemoryRaii() = delete;
        ~MemoryRaii() { _globalMt->_decrTotal(memSize); }

        size_t const memSize;
        friend class MemoryTracker;

    private:
        /// Only to be called by createRaii(), which locks the mutex.
        explicit MemoryRaii(size_t memSize_) : memSize(memSize_) { _globalMt->_incrTotal(memSize); }
    };
    friend class MemoryRaii;

    static void setup(std::string const& transferMethod, size_t max, std::string const& directory);
    static Ptr get() { return _globalMt; }

    /// Create a MemoryRaii instance to track `fileSize` bytes, and wait for free memory if `wait` is true.
    MemoryRaii::Ptr createRaii(size_t fileSize, bool wait);

    size_t getTotal() const {
        std::lock_guard lg(_mtx);
        return _total;
    }

    size_t getMax() const { return _max; }

    std::string getDirectory() const { return _directory; }

private:
    explicit MemoryTracker(TransferMethod transferMethod, size_t max, std::string const& directory)
            : _transferMethod(transferMethod), _max(max), _directory(directory) {}

    /// This function only to be called via createRaii.
    void _incrTotal(size_t sz);

    /// This function only to be called by ~MemoryRaii()
    void _decrTotal(size_t sz);

    static Ptr _globalMt;

    mutable std::mutex _mtx;
    std::condition_variable _cv;
    size_t _total = 0;
    TransferMethod const _transferMethod;
    size_t const _max;
    std::string const _directory;
};

class CsvStrMem : public CsvStream {
public:
    static std::shared_ptr<CsvStrMem> create(std::size_t expectedBytes) {
        return std::shared_ptr<CsvStrMem>(new CsvStrMem(expectedBytes));
    }

    CsvStrMem() = delete;
    CsvStrMem(CsvStrMem const&) = delete;
    CsvStream& operator=(CsvStream const&) = delete;
    ~CsvStrMem() override = default;

    void push(char const* data, std::size_t size) override;

    std::shared_ptr<std::string> pop() override;

    /// Wait if there isn't enough memory available.
    void waitReadyToRead() override;

    /// No thread to join.
    void join() override {};

protected:
    CsvStrMem(std::size_t expectedBytes) : CsvStream(expectedBytes + 1), _expectedBytes(expectedBytes) {};

    std::atomic<size_t> _bytesRead{0};
    size_t const _expectedBytes;

    MemoryTracker::MemoryRaii::Ptr _memRaii;
};

/// Store transfer data in memory until too much memory is being used.
/// By setting the maximum acceptable amount of memory to 0, this
/// effectively becomes writing results to disk.
/// Collecting data from the worker, writing it to disk, reading
/// it back, and merging is expected to be linear, run within a
/// single thread.
class CsvStrMemDisk : public CsvStrMem {
public:
    enum FileState { INIT, OPEN_W, CLOSE_W, OPEN_R, CLOSED };

    static std::shared_ptr<CsvStrMemDisk> create(std::size_t expectedBytes, QueryId qId, UberJobId ujId) {
        return std::shared_ptr<CsvStrMemDisk>(new CsvStrMemDisk(expectedBytes, qId, ujId));
    }

    CsvStrMemDisk() = delete;
    CsvStrMemDisk(CsvStrMemDisk const&) = delete;
    CsvStrMemDisk& operator=(CsvStrMemDisk const&) = delete;
    ~CsvStrMemDisk() override = default;

    void push(char const* data, std::size_t size) override;

    std::shared_ptr<std::string> pop() override;

    /// This version never waits.
    void waitReadyToRead() override;

    /// True if a file error happened before results would be contaminated.
    bool isFileError() const { return _fileError; }

private:
    CsvStrMemDisk(std::size_t expectedBytes, QueryId qId, UberJobId ujId);

    void _writeTofile(char const* data, std::size_t size);

    /// Read from the file, which should only happen after all writing has finished.
    std::shared_ptr<std::string> _readFromFile();

    bool _mustWriteToFile();

    /// Have at least on record ready to be pushed
    unsigned int const _minRecordsSize = 1;
    size_t const _minBytesInMem = 10'000'000;  // &&& config

    bool _writingToFile = false;
    std::string const _directory;
    std::string const _baseName;
    QueryId const _qId;
    UberJobId const _ujId;

    std::atomic<FileState> _fState = INIT;
    std::string _filePath;  ///< file path, constant once set.
    std::fstream _file;

    std::atomic<bool> _fileError = false;
    size_t _bytesLeft = 0;
};

}  // namespace lsst::qserv::mysql
#endif  // LSST_QSERV_MYSQL_CSVBUFFER_H
