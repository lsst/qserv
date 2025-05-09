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
#include <condition_variable>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>

// Third-party headers
#include <mysql/mysql.h>

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
    ~CsvStream() = default;

    /**
     * Push a new record to the stream. The record is a string of bytes.
     * Bytes will be copied into the newly created record.
     * The method will block if the stream is full until a record is popped.
     * The empty record (data == nullptr or size==0) should be inserted to indicate
     * stream termination.
     * @param data The record to be pushed to the stream
     * @param size The size of the record
     */
    void push(char const* data, std::size_t size);

    /**
     * Pop a record from the stream. The method will block if the stream is empty
     * until a record is pushed.
     * @return A shared pointer to the popped record or an empty string for the end of the stream
     */
    std::shared_ptr<std::string> pop();

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

private:
    CsvStream(std::size_t maxRecords);

    mutable std::mutex _mtx;
    std::condition_variable _cv;
    std::size_t const _maxRecords;
    std::list<std::shared_ptr<std::string>> _records;
    std::atomic<size_t> _bytesWritten;
};

/**
 * The factory function creates a new CsvBuffer object which will forward
 * the data pulled from the specified stream to a given buffer.
 * @param csvStream The input stream that us filled with the CSV-formatted bytestream
 * @return A shared pointer to the newly created object
 */
std::shared_ptr<CsvBuffer> newCsvStreamBuffer(std::shared_ptr<CsvStream> const& csvStream);

}  // namespace lsst::qserv::mysql
#endif  // LSST_QSERV_MYSQL_CSVBUFFER_H
