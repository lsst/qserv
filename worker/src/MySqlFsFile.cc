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
/// Implements MySqlFsFile, the primary way of interfacing qserv
/// worker functionality to an xrootd file-system interface.
 
#include "lsst/qserv/worker/MySqlFsFile.h"

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysError.hh"

#if DO_NOT_USE_BOOST
#include <regex.h>
#  include "XrdSys/XrdSysPthread.hh"
#  include "lsst/qserv/worker/Regex.h"
#  include "lsst/qserv/worker/format.h"
#else
#  include "boost/regex.hpp"
#  include "boost/thread.hpp"
#  include "boost/format.hpp"
#endif
#include "lsst/qserv/worker/Thread.h"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/MySqlFsCommon.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/RequestTaker.h"
#include "lsst/qserv/worker/Logger.h"
#include "lsst/qserv/QservPath.hh"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <functional>
#include <errno.h>
#include "mysql/mysql.h"
#include <numeric>
#include <unistd.h>
#include <sstream>
#include <iostream> // For file-scoped debug output

#define QSERV_USE_STUPID_STRING 1

using namespace lsst::qserv::worker;

// Boost launching helper
template <typename Callable>
void launchThread(Callable const& c) {
#if DO_NOT_USE_BOOST 
    ThreadDetail* td = newDetail<Callable>(c);
    ThreadManager::takeControl(td);
    Thread t(td);
#else
    boost::thread t(c);
#endif
}

class ReadCallable {
public:
    ReadCallable(MySqlFsFile& fsfile,
                 XrdSfsAio* aioparm)
        : _fsfile(fsfile), _aioparm(aioparm), _sfsAio(aioparm->sfsAio) {}

    void operator()() {
        _aioparm->Result = _fsfile.read(_sfsAio.aio_offset, 
                                        (char*)_sfsAio.aio_buf, 
                                        _sfsAio.aio_nbytes);
        _aioparm->doneRead();
    }
private:
    MySqlFsFile& _fsfile;
    XrdSfsAio* _aioparm;
    struct aiocb& _sfsAio;

};

class WriteCallable {
public:
    WriteCallable(MySqlFsFile& fsfile,
                  XrdSfsAio* aioparm, char* buffer)
        : _fsfile(fsfile), _aioparm(aioparm), _sfsAio(aioparm->sfsAio), 
          _buffer(buffer)
    {}

    void operator()() {
        // Check for mysql busy-ness.
        _sema.proberen();
        // Normal write
        _aioparm->Result = _fsfile.write(_sfsAio.aio_offset, 
                                         (char const*)_buffer, 
                                         _sfsAio.aio_nbytes);
        _sema.verhogen();
        delete[] _buffer;
        _buffer = 0;
        if(_aioparm->Result != (int)_sfsAio.aio_nbytes) {
            // overwrite error result with generic IO error?
            _aioparm->Result = -EIO;
        }
        _aioparm->doneWrite();
    }

private:
    MySqlFsFile& _fsfile;
    XrdSfsAio* _aioparm;
    struct aiocb& _sfsAio;
    char* _buffer;
    static Semaphore _sema;
};
// for now, two simultaneous writes (queries)
Semaphore WriteCallable::_sema(2);

bool flushOrQueue(QueryRunnerArg const& a)  {
    QueryRunner::Manager& mgr = QueryRunner::getMgr();
    mgr.runOrEnqueue(a);
    return true;
}

static int findChunkNumber(char const* path) {
    // path looks like: "/query/314159"
    // Idea: Choose last /-delimited element and try conversion.
    std::string p(path);
    int last = p.length()-1;
    // Strip trailing / if present
    if(p[last] == '/') --last;
    int first = p.rfind('/', last) + 1 ; // Move right of the found '/'
    std::string numberstring = p.substr(first, 1 + last - first);
    long result = strtol(numberstring.c_str(), 0, 10);
    return result;
}

class Timer { // duplicate of lsst::qserv:master::Timer
public:
    void start() { ::gettimeofday(&startTime, NULL); }
    void stop() { ::gettimeofday(&stopTime, NULL); }
    double getElapsed() const { 
        time_t seconds = stopTime.tv_sec - startTime.tv_sec;
        suseconds_t usec = stopTime.tv_usec - startTime.tv_usec;
        return seconds + (usec * 0.000001);
    }
    char const* getStartTimeStr() const {
        char* buf = const_cast<char*>(startTimeStr); // spiritually const
        asctime_r(localtime(&stopTime.tv_sec), buf); 
        buf[strlen(startTimeStr)-1] = 0;
        return startTimeStr;
    }

    char startTimeStr[30];
    struct ::timeval startTime;
    struct ::timeval stopTime;

    friend std::ostream& operator<<(std::ostream& os, Timer const& tm);
};

std::ostream& operator<<(std::ostream& os, Timer const& tm) {
    os << tm.getStartTimeStr() << " " << tm.getElapsed();
    return os;
}

//////////////////////////////////////////////////////////////////////////////
// MySqlFsFile
//////////////////////////////////////////////////////////////////////////////
MySqlFsFile::MySqlFsFile(boost::shared_ptr<Logger> log, 
                                  char const* user, 
                                  AddCallbackFunction::Ptr acf,
                                  fs::FileValidator::Ptr fv,
                                  boost::shared_ptr<Service> service) 
    : XrdSfsFile(user), 
      _log(log), 
      _addCallbackF(acf),
      _validator(fv),
      _service(service) {

    // Capture userName at this point.
    // Param user is: user.pid:fd@host
    // (See XRootd Protocol spec: 4.2.1.1 Connection name format)
    // Actually, master will pre-munge user as user.<mode>
    // where <mode> is "r" or "w".
    char const* cursor = user;
    while(cursor && (*cursor != '.')) ++cursor;
    _userName = std::string(user, cursor - user);
}

MySqlFsFile::~MySqlFsFile(void) {
}

int MySqlFsFile::_acceptFile(char const* fileName) {
    int rc;
    _path.reset(new QservPath(fileName));
    QservPath::RequestType rt = _path->requestType();
    switch(rt) {
    case QservPath::GARBAGE:
    case QservPath::UNKNOWN:
        _log->error((Pformat("Unrecognized file open %1% by %2%")
                   % fileName % _userName).str().c_str());
        return SFS_ERROR;

    case QservPath::OLDQ1:
        _log->error((Pformat("Unrecognized file open %1% by %2%")
                     % fileName % _userName).str().c_str());

        return SFS_ERROR; // Unimplemented
    case QservPath::CQUERY:
        // For now, use old validator.
        // _eDest->Say((Pformat("Newstyle open: %1% for chunk %2%")
        //              % fileName % _path->chunk()).str().c_str());
        if(!(*_validator)(fileName)) {
            error.setErrInfo(ENOENT, "File does not exist");
            _log->warn((Pformat("WARNING: unowned chunk query detected: %1%(%2%)")
                         % fileName % _chunkId ).str().c_str());
            return SFS_ERROR;        
        }
        _requestTaker.reset(new RequestTaker(_service->getAcceptor(),
                                             *_path));
        
        return SFS_OK; // No other action is needed.

    case QservPath::RESULT:
        rc = _checkForHash(_path->hashName());
        if(rc == SFS_ERROR) {
            _log->error((Pformat("File open %1% fail. Query error: %2%.")
                         % fileName % error.getErrText()).str().c_str());
        } else if (rc == SFS_OK) {
            _log->info((Pformat("File open %1% for result reading by %2%")
                       % fileName % _userName).str().c_str());
        }
        return rc;
    case QservPath::OLDQ2:
        _chunkId = _path->chunk();
        _log->info((Pformat("File open %1% for query invocation by %2%")
                     % fileName % _userName).str().c_str());
        if(!(*_validator)(fileName)) {
            error.setErrInfo(ENOENT, "File does not exist");
            _log->warn((Pformat("WARNING: unowned chunk query detected: %1%(%2%)")
                         % fileName % _chunkId ).str().c_str());
            return SFS_ERROR;        
        }
        return SFS_OK;
    default:
        _log->error((Pformat("Unrecognized file open %1% by %2%")
                     % fileName % _userName).str().c_str());
        return SFS_ERROR;
    }

}

int MySqlFsFile::open(char const* fileName, 
                               XrdSfsFileOpenMode openMode, 
                               mode_t createMode,
                               XrdSecEntity const* client, 
                               char const* opaque) {
    if (fileName == 0) {
        error.setErrInfo(EINVAL, "Null filename");
        return SFS_ERROR;
    }
    return _acceptFile(fileName);
}

int MySqlFsFile::close(void) {
    _log->info((Pformat("File close(%1%) by %2%")
               % _path->chunk() % _userName).str().c_str());
    if(_path->requestType() == QservPath::RESULT) {
        // Get rid of the news.
        std::string hash = fs::stripPath(_dumpName);
        QueryRunner::getTracker().clearNews(hash);
        
        // Must remove dump file while we are doing the single-query workaround
        // _eDest->Say((Pformat("Not Removing dump file(%1%)")
        // 		 % _dumpName ).str().c_str());
        int result = ::unlink(_dumpName.c_str());
        _log->info((Pformat("Unlink: %1%") % _dumpName ).str().c_str());
        if(result != 0) {
            _log->error((Pformat("Error removing dump file(%1%): %2%")
                        % _dumpName % strerror(errno)).str().c_str());
        }
    }
    return SFS_OK;
}

int MySqlFsFile::fctl(
    int const cmd, char const* args, XrdOucErrInfo& outError) {
    // if rewind: do something
    // else:
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

char const* MySqlFsFile::FName(void) {
    _log->info((Pformat("File FName(%1%) by %2%")
               % _path->chunk() % _userName).str().c_str());
    return 0;
}

int MySqlFsFile::getMmap(void** Addr, off_t &Size) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFsFile::read(XrdSfsFileOffset fileOffset,
                               XrdSfsXferSize prereadSz) {
    _hasRead = true;
    _log->info((Pformat("File read(%1%) at %2% by %3%")
               % _path->chunk() % fileOffset % _userName).str().c_str());
    if(_dumpName.empty()) { _setDumpNameAsChunkId(); }
    if (!dumpFileExists(_dumpName)) {
        std::string s = "Can't find dumpfile: " + _dumpName;
        _log->error(s);

        error.setErrInfo(ENOENT, "Query results missing");
        //return SFS_ERROR;
        return -ENOENT;
    }
    return SFS_OK;
}

XrdSfsXferSize MySqlFsFile::read(
    XrdSfsFileOffset fileOffset, char* buffer, XrdSfsXferSize bufferSize) {
    std::string msg;
    _hasRead = true;
    struct stat statbuf;
    if (::stat(_dumpName.c_str(), &statbuf) == -1) {
        statbuf.st_size = -errno;
    }
            
    msg = (Pformat("File read(%1%) at %2% for %3% by %4% [actual=%5% %6%]")
           % _path->chunk() % fileOffset % bufferSize % _userName 
           % _dumpName % statbuf.st_size).str();
    _log->info(msg);
    if(_dumpName.empty()) { _setDumpNameAsChunkId(); }
    int fd = dumpFileOpen(_dumpName);
    if (fd == -1) {
      std::stringstream ss;
      ss << (void*)this << "  Can't open dumpfile: " << _dumpName;
      _log->error(ss.str());

      error.setErrInfo(errno, "Query results missing");
        //return -1;
        return -errno;
    } else {
      std::stringstream ss;
      ss << (void*)this << "  Dumpfile OK: " << _dumpName;
      _log->info(ss.str());
    }
    off_t pos = ::lseek(fd, fileOffset, SEEK_SET);
    if (pos == static_cast<off_t>(-1) || pos != fileOffset) {
        error.setErrInfo(errno, "Unable to seek in query results");
        //return -1;
        return -errno;
    }
    ssize_t bytes = ::read(fd, buffer, bufferSize);
    if (bytes == -1) {
        error.setErrInfo(errno, "Unable to read query results");
        //return -1;
        return -errno;
    }
    ::close(fd);
    return bytes;
}

int MySqlFsFile::read(XrdSfsAio* aioparm) {
    _hasRead = true;
    // Spawn a throwaway thread that calls the normal, blocking read.
    launchThread(ReadCallable(*this, aioparm));
    return SFS_OK;
}

XrdSfsXferSize MySqlFsFile::write(
    XrdSfsFileOffset fileOffset, char const* buffer,
    XrdSfsXferSize bufferSize) {
    Timer t;
    std::stringstream ss;
    t.start();
    std::string descr((Pformat("File write(%1%) at %2% for %3% by %4%")
                       % _chunkId % fileOffset % bufferSize % _userName).str());
    _log->info(descr);
    //    return -EINVAL; // Garble for error testing.

    if (bufferSize <= 0) {
        error.setErrInfo(EINVAL, "No query provided");
        return -EINVAL;
    }
    if(_path->requestType() == QservPath::CQUERY) {
        if(_requestTaker->receive(fileOffset, buffer, bufferSize)) {
            if (_hasPacketEof(buffer, bufferSize)) {
                _requestTaker->complete();
            } else return bufferSize;
        } else return -EIO;
    }
    _addWritePacket(fileOffset, buffer, bufferSize);
    _log->info((Pformat("File write(%1%) Added.") % _chunkId).str());

    if(_hasPacketEof(buffer, bufferSize)) {
        _log->info((Pformat("File write(%1%) Flushing.") % _chunkId).str());
        bool querySuccess = _flushWrite();
        if(!querySuccess) {
            _log->info("Flush returned fail.");
            error.setErrInfo(EIO, "Error executing query.");
            //return -1;
            return -EIO;
        }
        _log->info("Flush ok, ready to return good.");
    }
    _log->info(descr + " --FINISH--");
    t.stop();
    ss << _chunkId << " WriteSpawn " << t;
    _log->info(ss.str());
    return bufferSize;
}
	
int MySqlFsFile::write(XrdSfsAio* aioparm) {
    aioparm->Result = write(aioparm->sfsAio.aio_offset, 
                            (const char*)aioparm->sfsAio.aio_buf,
                            aioparm->sfsAio.aio_nbytes);
    _log->info("AIO write.");

    if(aioparm->Result != (int)aioparm->sfsAio.aio_nbytes) {
        // overwrite error result with generic IO error?
        aioparm->Result = -EIO;
    }
    aioparm->doneWrite();
    return SFS_OK;
}

int MySqlFsFile::sync(void) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFsFile::sync(XrdSfsAio* aiop) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFsFile::stat(struct stat* buf) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFsFile::truncate(XrdSfsFileOffset fileOffset) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFsFile::getCXinfo(char cxtype[4], int &cxrsz) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

bool MySqlFsFile::_addWritePacket(XrdSfsFileOffset offset, 
                                           char const* buffer, 
                                           XrdSfsXferSize bufferSize) {
    _queryBuffer.addBuffer(offset, buffer, bufferSize);
    return true;
}

void MySqlFsFile::_addCallback(std::string const& filename) {
    assert(_path->requestType() == QservPath::RESULT);
    assert(_addCallbackF.get() != 0);
    (*_addCallbackF)(*this, filename);
}

ResultErrorPtr 
MySqlFsFile::_getResultState(std::string const& physFilename) {
    assert(_path->requestType() == QservPath::RESULT);
    // Lookup result hash.
    std::string hash = fs::stripPath(physFilename);
    //_eDest->Say(("Getting news for hash=" +hash).c_str());
    ResultErrorPtr p = QueryRunner::getTracker().getNews(hash);
    return p;
}

bool MySqlFsFile::_flushWrite() {
    switch(_path->requestType()) {
    case QservPath::CQUERY:
        //return _fs.addQuery(_queryBuffer);
        return true;
    case QservPath::OLDQ2:
        return _flushWriteDetach();
    case QservPath::OLDQ1:
        return _flushWriteSync();
    default:
        _log->error("Wrong filestate for writing. FIX THIS BUG.");
        _queryBuffer.reset(); // Kill the buffer.
        return false;
    }
    // switch should have already returned.
}

bool MySqlFsFile::_flushWriteDetach() {
    Task::Ptr t(new Task(ScriptMeta(_queryBuffer, _chunkId), _userName));
    QueryRunnerArg a(_log, t);
    return flushOrQueue(a);
}

bool MySqlFsFile::_flushWriteSync() {
    Task::Ptr t(new Task(ScriptMeta(_queryBuffer, _chunkId), _userName));
    _setDumpNameAsChunkId(); // Because reads may get detached from writes.
    //_eDest->Say((Pformat("db=%1%.") % s.dbName).str().c_str());
    QueryRunner runner(_log, t, _dumpName);
    return runner();
}

bool MySqlFsFile::_hasPacketEof(
    char const* buffer, XrdSfsXferSize bufferSize) const {
    if(bufferSize < 4) {
        return false;
    }
    char const* last4 = buffer-4+bufferSize;
    return ((last4[0] == '\0') &&
            (last4[1] == '\0') &&
            (last4[2] == '\0') &&
            (last4[3] == '\0'));
}

void MySqlFsFile::_setDumpNameAsChunkId() {
    // This can get deprecated soon.
    std::stringstream ss;
    ss << DUMP_BASE << _chunkId << ".dump";
    ss >> _dumpName;
}

int MySqlFsFile::_handleTwoReadOpen(char const* fileName) {
    std::string hash = fs::stripPath(fileName);
    return _checkForHash(hash);
}

int MySqlFsFile::_checkForHash(std::string const& hash) {
    _dumpName = hashToResultPath(hash); 
    _hasRead = false;
    ResultErrorPtr p = _getResultState(_dumpName);
    if(p.get()) {
        if(p->first != 0) { // Error, so report it.
            error.setErrInfo(EINVAL, p->second.c_str());
            return SFS_ERROR;
        }
    } else {
        _addCallback(hash);
        return SFS_STARTED;
    }
    return SFS_OK;
}
