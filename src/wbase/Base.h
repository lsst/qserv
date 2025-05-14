// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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

#ifndef LSST_QSERV_WBASE_BASE_H
#define LSST_QSERV_WBASE_BASE_H

// System headers
#include <deque>
#include <mutex>
#include <sstream>
#include <string>

// Third-party headers
#include "boost/format.hpp"

// Forward declarations
class XrdSysError;
class XrdSysLogger;
class XrdSfsAio;

namespace lsst::qserv::wbase {
class StringBuffer;
class StringBuffer2;
}  // namespace lsst::qserv::wbase

namespace lsst::qserv::wbase {

typedef long long StringBufferOffset;
typedef int StringBufferSize;

// Constants
extern std::string DUMP_BASE;  // Non-const to allow runtime-update via config
extern std::string const CREATE_SUBCHUNK_SCRIPT;
extern std::string const CLEANUP_SUBCHUNK_SCRIPT;
extern std::string const CREATE_DUMMY_SUBCHUNK_SCRIPT;

// Result-writing
void updateResultPath(char const* resultPath = 0);
void clearResultPath();

class StringBuffer {
public:
    StringBuffer() {}
    ~StringBuffer() { reset(); }
    void addBuffer(StringBufferOffset offset, char const* buffer, StringBufferSize bufferSize);
    std::string getStr() const;
    StringBufferOffset getLength() const;
    std::string getDigest() const;
    void reset();

private:
    struct Fragment {
        Fragment(StringBufferOffset offset_, char const* buffer_, StringBufferSize bufferSize_)
                : offset(offset_), buffer(buffer_), bufferSize(bufferSize_) {}

        StringBufferOffset offset;
        char const* buffer;
        StringBufferSize bufferSize;
    };

    typedef std::deque<Fragment> FragmentDeque;
    FragmentDeque _buffers;
    StringBufferOffset _totalSize{0};
    std::mutex _mutex;
    std::stringstream _ss;
};

class StringBuffer2 {
public:
    StringBuffer2() {}
    ~StringBuffer2() { reset(); }
    void addBuffer(StringBufferOffset offset, char const* buffer, StringBufferSize bufferSize);
    std::string getStr() const;
    StringBufferOffset getLength() const;
    char const* getData() const;
    void reset();

private:
    void _setSize(unsigned size);
    std::mutex _mutex;
    char* _buffer{nullptr};
    unsigned _bufferSize{0};
    unsigned _bytesWritten{0};
};

}  // namespace lsst::qserv::wbase

typedef boost::format Pformat;

#endif  // LSST_QSERV_WBASE_BASE_H
