// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 AURA/LSST.
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

// Basic convention/API-related things that might be shared.
//
// TODO:
//  Should parameterize things to stop hardcoding table names
// and column names.

// Class header
#include "wbase/Base.h"

// System headers
#include <cassert>
#include <cstddef>
#include <fstream>
#include <glob.h>
#include <iostream>
#include <sstream>
#include <string.h> // memcpy
#include <unistd.h>

// Third-party headers
#include "boost/format.hpp"

// Qserv headers
#include "global/constants.h"
#include "util/StringHash.h"


// Local helpers
namespace {
template <class T> struct ptrDestroy {
    void operator() (T& x) { delete[] x.buffer;}
};

template <class T> struct offsetLess {
    bool operator() (T const& x, T const& y) { return x.offset < y.offset;}
};
} // annonymous namespace

bool checkWritablePath(char const* path) {
    return path && (0 == ::access(path, W_OK | X_OK));
}

namespace lsst {
namespace qserv {
namespace wbase {

//////////////////////////////////////////////////////////////////////
// Constants
//////////////////////////////////////////////////////////////////////
// Must end in a slash.
std::string DUMP_BASE = "/tmp/qserv/";

std::string const SUBCHUNKDB_PREFIX_STR = SUBCHUNKDB_PREFIX;
// Parameters:
// %1% database (e.g., LSST)
// %2% table (e.g., Object)
// %3% subchunk column name (e.g. x_subChunkId)
// %4% chunkId (e.g. 2523)
// %5% subChunkId (e.g., 34)
std::string const CREATE_SUBCHUNK_SCRIPT =
    "CREATE DATABASE IF NOT EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%4%;"
    "CREATE TABLE IF NOT EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%4%.%2%_%4%_%5% ENGINE = MEMORY "
    "AS SELECT * FROM %1%.%2%_%4% WHERE %3% = %5%;"
    "CREATE TABLE IF NOT EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%4%.%2%FullOverlap_%4%_%5% "
    "ENGINE = MEMORY "
    "AS SELECT * FROM %1%.%2%FullOverlap_%4% WHERE %3% = %5%;";

// Parameters:
// %1% database (e.g., LSST)
// %2% table (e.g., Object)
// %3% chunkId (e.g. 2523)
// %4% subChunkId (e.g., 34)
std::string const CLEANUP_SUBCHUNK_SCRIPT =
    "DROP TABLE IF EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%3%.%2%_%3%_%4%;"
//    "DROP TABLE IF EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%3%.%2%SelfOverlap_%3%_%4%;"
    "DROP TABLE IF EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%3%.%2%FullOverlap_%3%_%4%;";

// Parameters:
// %1% database (e.g., LSST)
// %2% table (e.g., Object)
// %3% subchunk column name (e.g. x_subChunkId)
// %4% chunkId (e.g. 2523)
// %5% subChunkId (e.g., 34)
std::string const CREATE_DUMMY_SUBCHUNK_SCRIPT =
    "CREATE DATABASE IF NOT EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%4%;"
    "CREATE TABLE IF NOT EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%4%.%2%_%4%_%5% ENGINE = MEMORY "
    "AS SELECT * FROM %1%.%2%_%4% WHERE %3% = %5%;"
    "CREATE TABLE IF NOT EXISTS " + SUBCHUNKDB_PREFIX_STR + "%1%_%4%.%2%FullOverlap_%4%_%5% "
    "ENGINE = MEMORY "
    "AS SELECT * FROM %1%.%2%_%4% WHERE %3% = %5%;";

// Note:
// Not all Object partitions will have overlap tables created by the
// partitioner.  Thus we need to create empty overlap tables to prevent
// run-time errors.  The following command might be useful (put it on
// a single line):
//
// echo "show tables in LSST;" | mysql --socket=/u1/local/mysql.sock
// | grep Object_ | sed 's/\(.*\)_\(.*\)/create table if not exists LSST.
//

//////////////////////////////////////////////////////////////////////
// StringBuffer
//////////////////////////////////////////////////////////////////////
void StringBuffer::addBuffer(
    StringBufferOffset offset, char const* buffer, StringBufferSize bufferSize) {
    char* newItem = new char[bufferSize];
    assert(newItem != nullptr);
    memcpy(newItem, buffer, bufferSize);
    { // Assume(!) that there are no overlapping writes.
        std::unique_lock<std::mutex> lock(_mutex);
        _buffers.push_back(Fragment(offset, newItem, bufferSize));
        _totalSize += bufferSize;
    }
}

std::string StringBuffer::getStr() const {
    std::string accumulated;
    char* accStr = new char[_totalSize];
    assert(accStr);
    int cursor=0;
    if (false) {
        // Cast away const to perform a sort (doesn't logically change state)
        FragmentDeque& nonConst = const_cast<FragmentDeque&>(_buffers);
        std::sort(nonConst.begin(), nonConst.end(), offsetLess<Fragment>());
    }
    FragmentDeque::const_iterator bi;
    FragmentDeque::const_iterator bend = _buffers.end();

    //    accumulated.assign(getLength(), '\0'); //
    for(bi = _buffers.begin(); bi != bend; ++bi) {
        Fragment const& p = *bi;
        //accumulated += std::string(p.buffer, p.bufferSize);
        memcpy(accStr+cursor, p.buffer, p.bufferSize);
        cursor += p.bufferSize;
        // Perform "writes" of the buffers into the string
        // Assume that we end up with a contiguous string.
        //accumulated.replace(p.offset, p.bufferSize, p.buffer, p.bufferSize);
    }
    assert(cursor == _totalSize);
    accumulated.assign(accStr, cursor);
    delete[] accStr;
    return accumulated;
}

std::string StringBuffer::getDigest() const {
    FragmentDeque::const_iterator bi;
    FragmentDeque::const_iterator bend = _buffers.end();

    std::stringstream ss;
    for(bi = _buffers.begin(); bi != bend; ++bi) {
        Fragment const& p = *bi;
        ss << "Offset=" << p.offset << "\n";
        int fragsize = 100;
        if (fragsize > p.bufferSize) fragsize = p.bufferSize;
        ss << std::string(p.buffer, fragsize) << "\n";
    }
    return ss.str();
}

StringBufferOffset StringBuffer::getLength() const {
    return _totalSize;
    // Might be wise to do a sanity check sometime (overlapping writes!)
}

void StringBuffer::reset() {
    {
        std::unique_lock<std::mutex> lock(_mutex);
        std::for_each(_buffers.begin(), _buffers.end(), ptrDestroy<Fragment>());
        _buffers.clear();
    }
}

//////////////////////////////////////////////////////////////////////
// StringBuffer2
// A mutex-protected string buffer that uses a raw c-string.
//////////////////////////////////////////////////////////////////////
void StringBuffer2::addBuffer(
    StringBufferOffset offset, char const* buffer, StringBufferSize bufferSize) {
    std::unique_lock<std::mutex> lock(_mutex);
    if (_bufferSize < offset+bufferSize) {
        _setSize(offset+bufferSize);
    }
     memcpy(_buffer+offset, buffer, bufferSize);
    _bytesWritten += bufferSize;
}

std::string StringBuffer2::getStr() const {
    // Bad idea to call this if the buffer has holes.
    // Cast away const in order to lock.
    std::mutex& mutex = const_cast<std::mutex&>(_mutex);
    std::unique_lock<std::mutex> lock(mutex);
    assert(_bytesWritten == _bufferSize); //no holes.
    return std::string(_buffer, _bytesWritten);
}

char const* StringBuffer2::getData() const {
    // Don't call this unless the buffer has no holes.
    // Cast away const in order to lock.
    std::mutex& mutex = const_cast<std::mutex&>(_mutex);
    std::unique_lock<std::mutex> lock(mutex);
    assert(_bytesWritten == _bufferSize); //no holes.
    return _buffer;
}

StringBufferOffset StringBuffer2::getLength() const {
    return _bytesWritten;
}

void StringBuffer2::reset() {
    std::unique_lock<std::mutex> lock(_mutex);
    if (_buffer) {
        delete[] _buffer;
        _buffer = 0;
        _bufferSize = 0;
    }
    _bytesWritten = 0;
}

void StringBuffer2::_setSize(unsigned size) {
    if (size==0) {
        if (_buffer) {
            delete[] _buffer;
            _buffer = 0;
            _bufferSize = 0;
        }
        return;
    }
    char* newBuffer = new char[size];
    if (_buffer) {
        memcpy(newBuffer, _buffer, _bufferSize);
        delete[] _buffer;
    }
    _buffer = newBuffer;
    _bufferSize = size;
}

}}} // namespace
