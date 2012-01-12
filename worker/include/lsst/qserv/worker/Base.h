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
 
#ifndef LSST_QSERV_WORKER_BASE_H
#define LSST_QSERV_WORKER_BASE_H

// Std
#include <deque>
#include <string>
#include <sstream>
// Xrootd
#include "XrdSfs/XrdSfsInterface.hh"

#ifdef DO_NOT_USE_BOOST
#include "XrdSys/XrdSysPthread.hh"
#else
#include "boost/thread.hpp"
#include "boost/format.hpp"
#endif

class XrdSysError;
class XrdSysLogger;
class XrdSfsAio;

namespace lsst {
namespace qserv {
class TaskMsg;
namespace worker {
// Forward:
class StringBuffer;
class StringBuffer2;

// Constants
extern std::string DUMP_BASE;
extern std::string CREATE_SUBCHUNK_SCRIPT; 
extern std::string CLEANUP_SUBCHUNK_SCRIPT;

// Result-writing
void updateResultPath(char const* resultPath=0);
void clearResultPath();

// Hashing-related
std::string hashQuery(char const* buffer, int bufferSize);
std::string hashToPath(std::string const& hash);
std::string hashToResultPath(std::string const& hash);

struct ScriptMeta {
    ScriptMeta(StringBuffer const& b, int chunkId_);
    ScriptMeta(StringBuffer2 const& b, int chunkId_);
    std::string script;
    std::string hash;
    std::string dbName;
    std::string resultPath;
    int chunkId;
};

class CheckFlag {
public:
    virtual ~CheckFlag() {}
    virtual bool operator()() = 0;
};
 
class StringBuffer {
public:
    StringBuffer() : _totalSize(0) {}
    ~StringBuffer() { reset(); }
    void addBuffer(XrdSfsFileOffset offset, char const* buffer, 
                   XrdSfsXferSize bufferSize);
    std::string getStr() const;
    XrdSfsFileOffset getLength() const;
    std::string getDigest() const;
    void reset();
private:
    struct Fragment {
        Fragment(XrdSfsFileOffset offset_, char const* buffer_, 
                 XrdSfsXferSize bufferSize_) 
            : offset(offset_), buffer(buffer_), bufferSize(bufferSize_) {}

        XrdSfsFileOffset offset; 
        char const* buffer;
        XrdSfsXferSize bufferSize;
    };

    typedef std::deque<Fragment> FragmentDeque;
    FragmentDeque _buffers;
    XrdSfsFileOffset _totalSize;
#if DO_NOT_USE_BOOST
    XrdSysMutex _mutex;
#else
    boost::mutex _mutex;
#endif
    std::stringstream _ss;
    };

class StringBuffer2 {
public:
    StringBuffer2() : _buffer(0), 
                      _bufferSize(0),_bytesWritten(0) {}
    ~StringBuffer2() { reset(); }
    void addBuffer(XrdSfsFileOffset offset, char const* buffer, 
                   XrdSfsXferSize bufferSize);
    std::string getStr() const;
    XrdSfsFileOffset getLength() const;
    char const* getData() const;
    void reset();
private:
    void _setSize(unsigned size);
#if DO_NOT_USE_BOOST
    XrdSysMutex _mutex;
#else
    boost::mutex _mutex;
#endif
    char* _buffer;
    unsigned _bufferSize;
    unsigned _bytesWritten;
};

class TaskAcceptor {
public:
    typedef boost::shared_ptr<TaskAcceptor> Ptr;

    TaskAcceptor() {}
    virtual bool accept(boost::shared_ptr<TaskMsg> msg) = 0;
};

class Logger {
public:
    virtual ~Logger() {}
    virtual void operator()(std::string const& s) = 0;
    virtual void operator()(char const* s) = 0;
};

}}}

#if DO_
NOT_USE_BOOST
typedef lsst::qserv::worker::PosFormat Pformat;
#else  
typedef boost::format Pformat;
#endif

#endif // LSST_QSERV_WORKER_BASE_H
