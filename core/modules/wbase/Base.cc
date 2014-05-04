/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

#include "wbase/Base.h"

// System headers
#ifdef __SUNPRO_CC
#include <sys/md5.h>
#else // Linux?
#include <openssl/md5.h>
#endif
#include <fstream>
#include <glob.h>
#include <iostream>

// Third-party headers
#include <boost/format.hpp>

// Local headers
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

// Parameters:
// %1% database (e.g., LSST)
// %2% table (e.g., Object)
// %3% subchunk column name (e.g. x_subChunkId)
// %4% chunkId (e.g. 2523)
// %5% subChunkId (e.g., 34)
std::string CREATE_SUBCHUNK_SCRIPT =
    "CREATE DATABASE IF NOT EXISTS Subchunks_%1%_%4%;"
    "CREATE TABLE IF NOT EXISTS Subchunks_%1%_%4%.%2%_%4%_%5% ENGINE = MEMORY "
    "AS SELECT * FROM %1%.%2%_%4% WHERE %3% = %5%;"
    "CREATE TABLE IF NOT EXISTS Subchunks_%1%_%4%.%2%FullOverlap_%4%_%5% "
    "ENGINE = MEMORY "
    "AS SELECT * FROM %1%.%2%FullOverlap_%4% WHERE %3% = %5%;"
    ;

// Parameters:
// %1% database (e.g., LSST)
// %2% table (e.g., Object)
// %3% chunkId (e.g. 2523)
// %4% subChunkId (e.g., 34)
std::string CLEANUP_SUBCHUNK_SCRIPT =
    "DROP TABLE IF EXISTS Subchunks_%1%_%3%.%2%_%3%_%4%;"
//    "DROP TABLE IF EXISTS Subchunks_%1%_%3%.%2%SelfOverlap_%3%_%4%;"
    "DROP TABLE IF EXISTS Subchunks_%1%_%3%.%2%FullOverlap_%3%_%4%;"
    ;

// Note:
// Not all Object partitions will have overlap tables created by the
// partitioner.  Thus we need to create empty overlap tables to prevent
// run-time errors.  The following command might be useful:
//
// echo "show tables in LSST;" | mysql --socket=/u1/local/mysql.sock  \
// | grep Object_ | sed 's/\(.*\)_\(.*\)/create table if not exists LSST.
//

//////////////////////////////////////////////////////////////////////
// Hashing-related
//////////////////////////////////////////////////////////////////////
#ifdef __SUNPRO_CC // MD5(...) not defined on Solaris's ssl impl.
namespace {
    inline unsigned char* MD5(unsigned char const* d,
                              unsigned long n,
                              unsigned char* md) {
        // Defined with RFC 1321 MD5 functions.
        MD5_CTX ctx;
        assert(md != NULL); // Don't support null input.
        MD5Init(&ctx);
        MD5Update(&ctx, d, n);
        MD5Final(md, &ctx);
        return md;
    }
}
#endif

void updateResultPath(char const* resultPath) {
    if(checkWritablePath(resultPath)) {
        DUMP_BASE.assign(resultPath);
        return;
    }
    char* path =::getenv("QSW_RESULTPATH");
    if(checkWritablePath(path)) {
        DUMP_BASE.assign(path);
    }
}

void clearResultPath() {
    // Conceptually: rm DUMP_BASE/*
    glob_t globbuf;
    std::string globstr(DUMP_BASE);
    globstr += "/*";
    // Glob, with no special opts, no error function
    if(0 == glob(globstr.c_str(), 0, NULL, &globbuf)) {
        char** s = globbuf.gl_pathv;
        while(0 != *s) {
            unlink(*s++); // delete file, ignore errors.
        }
        globfree(&globbuf);
    }
}

std::string hashToPath(std::string const& hash) {
    return DUMP_BASE +
        hash.substr(0, 3) + "/" + hash.substr(3, 3) + "/" + hash + ".dump";
}

std::string hashToResultPath(std::string const& hash) {
    // Not sure whether we want a different path later.
    // For now, drop the .dump extension.
    //    return DUMP_BASE +
    //        hash.substr(0, 3) + "/" + hash.substr(3, 3) + "/" + hash;
    // And drop the two-level directory to keep client complexity down since
    // xrootd seems to check raw paths.
    return DUMP_BASE + "/" + hash;
}

//////////////////////////////////////////////////////////////////////
// ScriptMeta
//////////////////////////////////////////////////////////////////////
ScriptMeta::ScriptMeta(StringBuffer const& b, int chunkId_) {
    script = b.getStr();
    hash = util::StringHash::getMd5Hex(script.data(), script.length());
    dbName = "q_" + hash;
    resultPath = hashToResultPath(hash);
    chunkId = chunkId_;
}

ScriptMeta::ScriptMeta(StringBuffer2 const& b, int chunkId_) {
    script = b.getStr();
    hash = util::StringHash::getMd5Hex(script.data(), script.length());
    dbName = "q_" + hash;
    resultPath = hashToResultPath(hash);
    chunkId = chunkId_;
}

//////////////////////////////////////////////////////////////////////
// StringBuffer
//////////////////////////////////////////////////////////////////////
void StringBuffer::addBuffer(
    StringBufferOffset offset, char const* buffer, StringBufferSize bufferSize) {
#if QSERV_USE_STUPID_STRING
#  if DO_NOT_USE_BOOST
    UniqueLock lock(_mutex);
#  else
    boost::unique_lock<boost::mutex> lock(_mutex);
#  endif
    _ss << std::string(buffer,bufferSize);
    _totalSize += bufferSize;
#else
    char* newItem = new char[bufferSize];
    assert(newItem != (char*)0);
    memcpy(newItem, buffer, bufferSize);
    { // Assume(!) that there are no overlapping writes.
#  if DO_NOT_USE_BOOST
        UniqueLock lock(_mutex);
#  else
        boost::unique_lock<boost::mutex> lock(_mutex);
#  endif
        _buffers.push_back(Fragment(offset, newItem, bufferSize));
        _totalSize += bufferSize;
    }
#endif
}

std::string StringBuffer::getStr() const {
#if QSERV_USE_STUPID_STRING
    // Cast away const in order to lock.
#   if DO_NOT_USE_BOOST
    UniqueLock lock(const_cast<XrdSysMutex&>(_mutex));
#   else
    boost::mutex& mutex = const_cast<boost::mutex&>(_mutex);
    boost::unique_lock<boost::mutex> lock(mutex);
#   endif
    return _ss.str();
#else
    std::string accumulated;
    char* accStr = new char[_totalSize];
    assert(accStr);
    int cursor=0;
    if(false) {
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
#endif
}

std::string StringBuffer::getDigest() const {
#if QSERV_USE_STUPID_STRING
    // Cast away const in order to lock.
#if DO_NOT_USE_BOOST
    UniqueLock lock(const_cast<XrdSysMutex&>(_mutex));
#else
    boost::mutex& mutex = const_cast<boost::mutex&>(_mutex);
    boost::unique_lock<boost::mutex> lock(mutex);
#endif
    int length = 200;
    if(length > _totalSize)
        length = _totalSize;

    return std::string(_ss.str().data(), length);
#else
    FragmentDeque::const_iterator bi;
    FragmentDeque::const_iterator bend = _buffers.end();

    std::stringstream ss;
    for(bi = _buffers.begin(); bi != bend; ++bi) {
        Fragment const& p = *bi;
        ss << "Offset=" << p.offset << "\n";
        int fragsize = 100;
        if(fragsize > p.bufferSize) fragsize = p.bufferSize;
        ss << std::string(p.buffer, fragsize) << "\n";
    }
    return ss.str();
#endif
}

StringBufferOffset StringBuffer::getLength() const {
    return _totalSize;
    // Might be wise to do a sanity check sometime (overlapping writes!)
#if 0
    struct accumulateSize {
        StringBufferSize operator() (StringBufferOffset x, Fragment const& p) {
            return x + p.bufferSize;
        }
    };
    return std::accumulate(_buffers.begin(), _buffers.end(),
                           0, accumulateSize());
#endif
}

void StringBuffer::reset() {
    {
#if DO_NOT_USE_BOOST
        UniqueLock lock(_mutex);
#else
        boost::unique_lock<boost::mutex> lock(_mutex);
#endif
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

#  if DO_NOT_USE_BOOST
    UniqueLock lock(_mutex);
#  else
    boost::unique_lock<boost::mutex> lock(_mutex);
#  endif
    if(_bufferSize < offset+bufferSize) {
        _setSize(offset+bufferSize);
    }
     memcpy(_buffer+offset, buffer, bufferSize);
    _bytesWritten += bufferSize;
}

std::string StringBuffer2::getStr() const {
    // Bad idea to call this if the buffer has holes.
    // Cast away const in order to lock.
#if DO_NOT_USE_BOOST
    UniqueLock lock(const_cast<XrdSysMutex&>(_mutex));
#else
    boost::mutex& mutex = const_cast<boost::mutex&>(_mutex);
    boost::unique_lock<boost::mutex> lock(mutex);
#endif
    assert(_bytesWritten == _bufferSize); //no holes.
    return std::string(_buffer, _bytesWritten);
}

char const* StringBuffer2::getData() const {
    // Don't call this unless the buffer has no holes.
    // Cast away const in order to lock.
#if DO_NOT_USE_BOOST
    UniqueLock lock(const_cast<XrdSysMutex&>(_mutex));
#else
    boost::mutex& mutex = const_cast<boost::mutex&>(_mutex);
    boost::unique_lock<boost::mutex> lock(mutex);
#endif
    assert(_bytesWritten == _bufferSize); //no holes.
    return _buffer;
}

StringBufferOffset StringBuffer2::getLength() const {
    return _bytesWritten;
}

void StringBuffer2::reset() {
#if DO_NOT_USE_BOOST
    UniqueLock lock(_mutex);
#else
    boost::unique_lock<boost::mutex> lock(_mutex);
#endif
    if(_buffer) {
        delete[] _buffer;
        _buffer = 0;
        _bufferSize = 0;
    }
    _bytesWritten = 0;
}

void StringBuffer2::_setSize(unsigned size) {
    if(size==0) {
        if(_buffer) {
            delete[] _buffer;
            _buffer = 0;
            _bufferSize = 0;
        }
        return;
    }
    char* newBuffer = new char[size];
    if(_buffer) {
        memcpy(newBuffer, _buffer, _bufferSize);
        delete[] _buffer;
    }
    _buffer = newBuffer;
    _bufferSize = size;
}

}}} // namespace lsst::qserv::wbase
