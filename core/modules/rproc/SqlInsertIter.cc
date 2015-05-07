// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2015 AURA/LSST.
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

// class SqlInsertIter -- A class that finds INSERT statements in
// mysqldump output and iterates over them.
// Should become obsolete with new  dump-less result transfer processing

// Class header
#include "rproc/SqlInsertIter.h"

// System headers
#include <cassert>
#include <errno.h>
#include <iostream>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"


namespace {

// Helpers to make regex's
boost::regex makeLockInsertRegex(std::string const& tableName) {
    return boost::regex("LOCK TABLES `?" + tableName + "`? WRITE;"
                        "(.*?)(INSERT INTO[^;]*?;)+(.*?)"
                        "UNLOCK TABLES;");
}

boost::regex makeLockInsertOpenRegex(std::string const& tableName) {
    return boost::regex("LOCK TABLES `?" + tableName + "`? WRITE;"
                        "(.*?)(INSERT INTO[^;]*?;)+");
}

boost::regex makeLockOpenRegex(std::string const& tableName) {
    return boost::regex("LOCK TABLES `?" + tableName + "`? WRITE;");
}

boost::regex makeInsertRegex(std::string const& tableName) {
    return boost::regex("(INSERT INTO `?" + tableName +
                        "`? [^;]+?;)");// [~;]*?;)");
}

boost::regex makeNullInsertRegex(std::string const& tableName) {
    return boost::regex("(INSERT INTO `?" + tableName +
                        "`? +VALUES ?[(]NULL(,NULL)*[)];)");
}

// Helpful debugging
void printInserts(char const* buf, off_t bufSize,
                  std::string const& tableName)  {
    for(lsst::qserv::rproc::SqlInsertIter i(buf, bufSize, tableName, true);
        !i.isDone();
        ++i) {
        std::cout << "Sql[" << tableName << "]: "
                   << (void*)i->first << "  --->  "
                   << (void*)i->second << "  "
                   << *i;
        if(i.isNullInsert()) {
            std::cout << "Null match" << std::endl;
        } else { std::cout << std::endl; }
    }
}

} // anonymous namespace


namespace lsst {
namespace qserv {
namespace rproc {

class SqlInsertIter::BufferMgr {
public:
    typedef unsigned long long BufOff;
    explicit BufferMgr(util::PacketBuffer::Ptr p)
        : pacBuffer(p) {_setup(); }

    ~BufferMgr() {
        if(buffer) {
            ::free(buffer);
            buffer = 0;
        }
    }
    void _setup() {
        assert(!(*pacBuffer).isDone());
        offStart = 0;
        offEnd = (*pacBuffer)->second;
        bufSize = 2 * offEnd; // Size to 2x first fragment size
        // (which may be bigger than average)
        buffer = static_cast<char*>(malloc(bufSize));

        memcpy(buffer,(*pacBuffer)->first, offEnd);

    }
    char* getStart() { return buffer + offStart; }
    char* getEnd() { return buffer + offEnd; }
    bool isDone() const { return pacBuffer->isDone(); }

    void advanceTo(char* newStart) {
        // Set offStart accordingly.
        offStart = newStart - buffer;
    }

    bool incrementFragment() {
        // Advance iterator.
        ++(*pacBuffer);
        if(pacBuffer->isDone()) return false; // Any more?
        util::PacketBuffer::Value v = **pacBuffer;
        // Make sure there is room in the buffer
        BufOff keepSize = offEnd - offStart;
        BufOff needSize = v.second + keepSize;
        if(needSize > (bufSize - offEnd)) {
            if(needSize > bufSize) {
                LOGF_DEBUG("%1% is too small. sqliter Realloc to %2%" %
                           bufSize % needSize);
                void* res = realloc(buffer, needSize);
                if (!res) {
                    errno = ENOMEM;
                    throw "Failed to realloc for SqlInsertIter.";
                }
                bufSize = needSize;
                buffer = static_cast<char*>(res);
            }
            // Move the part we care about to the beginning of the buffer.
            memmove(buffer, getStart(), keepSize);
            offEnd = keepSize;
            offStart = 0;
        }
        // Copy from PacketBuffer into own buffer.
        memcpy(getEnd(), v.first, v.second);
        offEnd += v.second;
        return true;
    }

    util::PacketBuffer::Ptr pacBuffer;
    char* buffer;
    BufOff bufSize;
    BufOff offStart; // Start of non-junk in buffer
    BufOff offEnd; // End of non-junk in buffer
};


////////////////////////////////////////////////////////////////////////
// SqlInsertIter
////////////////////////////////////////////////////////////////////////
// Static
SqlInsertIter::Iter SqlInsertIter::_nullIter;

SqlInsertIter::SqlInsertIter(char const* buf, off_t bufSize,
                             std::string const& tableName,
                             bool allowNull)
    : _allowNull(allowNull), _lastUsed(0) {
    _blockExpr = makeLockInsertRegex(tableName);
    _init(buf, bufSize, tableName);
}

SqlInsertIter::SqlInsertIter(util::PacketBuffer::Ptr p,
                             std::string const& tableName,
                             bool allowNull)
    : _allowNull(allowNull),
      _bufferMgr(std::make_shared<BufferMgr>(p)) {

    // We will need to keep our own buffer.  This is because the regex
    // iterator needs a continuous piece of memory.

    // The idea is to keep a sliding window where we can run the regex search.
    // While blocks can be found in the block, iterate over them.
    // When the search fails, remember where the last match
    // terminated.
    // Now increment the packet iter.
    // realloc the buffer to fit the unmatched remainder +
    // packetIter's advance (if needed). Note that buffer may be
    // bigger than what we need.
    // slide the unmatched to the beginning, memcpy the packetIter
    // data into our buffer, and setup the regex match again.
    // Continue.
    LOGF_DEBUG("EXECUTING SqlInsertIter(PacketIter::Ptr, %1%, %2%)"
               % tableName % allowNull);
    boost::regex lockInsertExpr(makeLockInsertOpenRegex(tableName));
    boost::regex lockExpr(makeLockOpenRegex(tableName));
    bool found = false;
    while(!found) {
        // need to add const to help compiler
        char const* buf = _bufferMgr->getStart();
        char const* bufEnd = _bufferMgr->getEnd();
        found = boost::regex_search(buf, bufEnd, _blockMatch, lockInsertExpr);
        if(found) {
            LOGF_DEBUG("Matched Lock statement within SqlInsertIter");
            break;
        } else {
            LOGF_DEBUG("Did not match Lock statement within SqlInsertIter");
        }
        //Add next fragment, if available.
        if(!_bufferMgr->incrementFragment()) {
            // Verify presence of Lock statement.
            buf = _bufferMgr->getStart();
            bufEnd = _bufferMgr->getEnd();
            if(boost::regex_search(buf, bufEnd, _blockMatch, lockExpr)) {
                return;
            } else {
                errno = ENOTRECOVERABLE;
                // FIXME need a real exception here
                throw "Failed to match Lock statement within SqlInsertIter.";
            }
        }
    }
    _blockFound = found;
    _initRegex(tableName);
    // Might try _blockMatch[3].first, _blockMatch[3].second
    _resetMgrIter();
}

SqlInsertIter::~SqlInsertIter() {
}

void SqlInsertIter::_resetMgrIter() {
    _iter = Iter(_bufferMgr->getStart(), _bufferMgr->getEnd(), _insExpr);
}

void SqlInsertIter::_initRegex(std::string const& tableName) {
    _insExpr = makeInsertRegex(tableName);
    _nullExpr = makeNullInsertRegex(tableName);
}

void SqlInsertIter::_init(char const* buf, off_t bufSize,
                          std::string const& tableName) {
    boost::regex lockInsertRegex(makeLockInsertRegex(tableName));
    assert(buf < (buf+bufSize));
    _blockFound = boost::regex_search(buf, buf+bufSize,
                                      _blockMatch, lockInsertRegex);
    if(_blockFound) {
        _initRegex(tableName);
        _iter = Iter(_blockMatch[2].first, _blockMatch[3].second,
                     _insExpr);
        _lastUsed = (*_iter)[0].second;
    } else {
        _iter = _nullIter;
    }
}

bool SqlInsertIter::isNullInsert() const {
    // Avoid constructing a string > 1MB just to check for null.
    if(_iter->length() > (1<<20)) return false;
    return boost::regex_match(_iter->str(), _nullExpr);
}

SqlInsertIter& SqlInsertIter::operator++() {
    do {
        _increment();
    } while(!isDone() && (!_allowNull) && isNullInsert());
        return *this;
}

bool SqlInsertIter::isDone() const {
    if(_bufferMgr) {
        return (_iter == _nullIter) || _bufferMgr->isDone();
    } else {
        return _iter == _nullIter;
    }
}

/// Increment this iterator to point at the next INSERT statement.
/// If our buffer includes the full data dump, then this is easy--we
/// can just advance the regex iterator _iter.  However, when we are
/// iterating over the dump in "packets", we may need to advance
/// the packet iterator.
void SqlInsertIter::_increment() {
    if(_bufferMgr) {
        // Set _pBufStart to end of last match.
        _bufferMgr->advanceTo(const_cast<char*>((*_iter)[0].second));
        //_pBufStart = static_cast<BufOff>((*_iter)[0].second - _pBuffer);
        ++_iter; // Advance the regex to the next INSERT stmt
        while((_iter == _nullIter) && !_bufferMgr->isDone()) {
            _bufferMgr->incrementFragment(); // Extend buffer
            _resetMgrIter(); // Reset the iterator.
        }
        // Either we found an insert or there are no more packets.
    } else { // If fully buffered.
        ++_iter;
        if(_iter != _nullIter) {
            _lastUsed = (*_iter)[0].second;
        }
    }
}

}}} // namespace lsst::qserv::rproc
