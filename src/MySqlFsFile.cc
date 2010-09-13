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

namespace qWorker = lsst::qserv::worker;


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
    ReadCallable(qWorker::MySqlFsFile& fsfile,
		    XrdSfsAio* aioparm)
	: _fsfile(fsfile), _aioparm(aioparm), _sfsAio(aioparm->sfsAio) {}

    void operator()() {
	_aioparm->Result = _fsfile.read(_sfsAio.aio_offset, 
				      (char*)_sfsAio.aio_buf, 
				      _sfsAio.aio_nbytes);
	_aioparm->doneRead();
    }
private:
    qWorker::MySqlFsFile& _fsfile;
    XrdSfsAio* _aioparm;
    struct aiocb& _sfsAio;

};

class WriteCallable {
public:
    WriteCallable(qWorker::MySqlFsFile& fsfile,
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
    qWorker::MySqlFsFile& _fsfile;
    XrdSfsAio* _aioparm;
    struct aiocb& _sfsAio;
    char* _buffer;
    static qWorker::Semaphore _sema;
};
// for now, two simultaneous writes (queries)
qWorker::Semaphore WriteCallable::_sema(2);


bool flushOrQueue(qWorker::QueryRunnerArg const& a)  {
    qWorker::QueryRunner::Manager& mgr = qWorker::QueryRunner::getMgr();
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
qWorker::MySqlFsFile::MySqlFsFile(XrdSysError* lp, char* user, 
				  AddCallbackFunction::Ptr acf) :
    XrdSfsFile(user), _eDest(lp), _addCallbackF(acf) {

    // Capture userName at this point.
    // Param user is: user.pid:fd@host 
    // (See XRootd Protocol spec: 4.2.1.1 Connection name format)
    char* cursor = user;
    while(cursor && (*cursor != '.')) ++cursor;
    _userName = std::string(user, cursor - user);
}

qWorker::MySqlFsFile::~MySqlFsFile(void) {
}

int qWorker::MySqlFsFile::open(
    char const* fileName, XrdSfsFileOpenMode openMode, mode_t createMode,
    XrdSecEntity const* client, char const* opaque) {

    int rc;

    if (fileName == 0) {
        error.setErrInfo(EINVAL, "Null filename");
        return SFS_ERROR;
    }
    _fileClass = fs::computeFileClass(fileName);
    switch(_fileClass) {
    case fs::COMBO:
	_chunkId = findChunkNumber(fileName);
	_eDest->Say((Pformat("File open %1%(%2%) by %3%")
		     % fileName % _chunkId % _userName).str().c_str());
	break;
    case fs::TWO_WRITE:
	_chunkId = findChunkNumber(fileName);
	_eDest->Say((Pformat("File open %1% for query invocation by %2%")
		     % fileName % _userName).str().c_str());
	break;
    case fs::TWO_READ:
	rc = _handleTwoReadOpen(fileName);
	if(rc != SFS_OK) {
	    return rc;
	}
	break;
    default:
	_eDest->Say((Pformat("Unrecognized file open %1% by %2%")
		     % fileName % _userName).str().c_str());
	return SFS_ERROR;
    }
    return SFS_OK;
}

int qWorker::MySqlFsFile::close(void) {
    _eDest->Say((Pformat("File close(%1%) by %2%")
                 % _chunkId % _userName).str().c_str());
    if((_fileClass == fs::COMBO)  ||
       ((_fileClass == fs::TWO_READ) && _hasRead) )	{
	// Get rid of the news.
	std::string hash = fs::stripPath(_dumpName);
	QueryRunner::getTracker().clearNews(hash);

	// Must remove dump file while we are doing the single-query workaround
	// _eDest->Say((Pformat("Not Removing dump file(%1%)")
	// 		 % _dumpName ).str().c_str());
	int result = ::unlink(_dumpName.c_str());
	if(result != 0) {
	    _eDest->Say((Pformat("Error removing dump file(%1%): %2%")
			 % _dumpName % strerror(errno)).str().c_str());
	}
    }
    return SFS_OK;
}

int qWorker::MySqlFsFile::fctl(
    int const cmd, char const* args, XrdOucErrInfo& outError) {
    // if rewind: do something
    // else:
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

char const* qWorker::MySqlFsFile::FName(void) {
    _eDest->Say((Pformat("File FName(%1%) by %2%")
                 % _chunkId % _userName).str().c_str());
    return 0;
}

int qWorker::MySqlFsFile::getMmap(void** Addr, off_t &Size) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::read(XrdSfsFileOffset fileOffset,
                          XrdSfsXferSize prereadSz) {
    _hasRead = true;
    _eDest->Say((Pformat("File read(%1%) at %2% by %3%")
                 % _chunkId % fileOffset % _userName).str().c_str());
    if(_dumpName.empty()) { _setDumpNameAsChunkId(); }
    if (!qWorker::dumpFileExists(_dumpName)) {
      std::string s = "Can't find dumpfile: " + _dumpName;
      _eDest->Say(s.c_str());

        error.setErrInfo(ENOENT, "Query results missing");
        //return SFS_ERROR;
	return -ENOENT;
    }
    return SFS_OK;
}

XrdSfsXferSize qWorker::MySqlFsFile::read(
    XrdSfsFileOffset fileOffset, char* buffer, XrdSfsXferSize bufferSize) {
    std::string msg;
    _hasRead = true;
    msg = (Pformat("File read(%1%) at %2% for %3% by %4% [actual=%5%]")
	   % _chunkId % fileOffset % bufferSize % _userName % _dumpName).str();
    _eDest->Say(msg.c_str());
    if(_dumpName.empty()) { _setDumpNameAsChunkId(); }
    int fd = qWorker::dumpFileOpen(_dumpName);
    if (fd == -1) {
      std::stringstream ss;
      ss << (void*)this << "  Can't open dumpfile: " << _dumpName;
      std::string s = ss.str();
      _eDest->Say(s.c_str());

        error.setErrInfo(errno, "Query results missing");
	//return -1;
        return -errno;
    } else {
      std::stringstream ss;
      ss << (void*)this << "  Dumpfile OK: " << _dumpName;
      std::string s;
      ss >> s;
      _eDest->Say(s.c_str());
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

int qWorker::MySqlFsFile::read(XrdSfsAio* aioparm) {
    _hasRead = true;
    // Spawn a throwaway thread that calls the normal, blocking read.
    launchThread(ReadCallable(*this, aioparm));
    return SFS_OK;
}

XrdSfsXferSize qWorker::MySqlFsFile::write(
    XrdSfsFileOffset fileOffset, char const* buffer,
    XrdSfsXferSize bufferSize) {
    Timer t;
    std::stringstream ss;
    t.start();
    std::string descr((Pformat("File write(%1%) at %2% for %3% by %4%")
		       % _chunkId % fileOffset % bufferSize % _userName).str());
    _eDest->Say(descr.c_str());
    //    return -EINVAL; // Garble for error testing.

    if (bufferSize <= 0) {
        error.setErrInfo(EINVAL, "No query provided");
	return -EINVAL;
    }
    _addWritePacket(fileOffset, buffer, bufferSize);
    _eDest->Say((Pformat("File write(%1%) Added.") % _chunkId).str().c_str());

    if(_hasPacketEof(buffer, bufferSize)) {
	_eDest->Say((Pformat("File write(%1%) Flushing.") % _chunkId).str().c_str());
	bool querySuccess = _flushWrite();
	if(!querySuccess) {
	    _eDest->Say("Flush returned fail.");
	    error.setErrInfo(EIO, "Error executing query.");
	    //return -1;
	    return -EIO;
	}
	_eDest->Say("Flush ok, ready to return good.");

    }
    _eDest->Say((descr + " --FINISH--").c_str());
    t.stop();
    ss << _chunkId << " WriteSpawn " << t;
    std::string sst(ss.str());
    _eDest->Say(sst.c_str());
    return bufferSize;
}

	
int qWorker::MySqlFsFile::write(XrdSfsAio* aioparm) {
    aioparm->Result = write(aioparm->sfsAio.aio_offset, 
			    (const char*)aioparm->sfsAio.aio_buf,
			    aioparm->sfsAio.aio_nbytes);
    _eDest->Say("AIO write.");

    if(aioparm->Result != (int)aioparm->sfsAio.aio_nbytes) {
	// overwrite error result with generic IO error?
	aioparm->Result = -EIO;
    }
    aioparm->doneWrite();
    return SFS_OK;
}

int qWorker::MySqlFsFile::sync(void) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::sync(XrdSfsAio* aiop) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::stat(struct stat* buf) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::truncate(XrdSfsFileOffset fileOffset) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFsFile::getCXinfo(char cxtype[4], int &cxrsz) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}



bool qWorker::MySqlFsFile::_addWritePacket(XrdSfsFileOffset offset, 
					   char const* buffer, 
					   XrdSfsXferSize bufferSize) {
    _queryBuffer.addBuffer(offset, buffer, bufferSize);
    return true;
}

void qWorker::MySqlFsFile::_addCallback(std::string const& filename) {
    assert(_fileClass == fs::TWO_READ);
    assert(_addCallbackF.get() != 0);
    (*_addCallbackF)(*this, filename);
}

qWorker::ResultErrorPtr qWorker::MySqlFsFile::_getResultState(std::string const& physFilename) {
    assert(_fileClass == fs::TWO_READ);
    // Lookup result hash.
    std::string hash = fs::stripPath(physFilename);
    //_eDest->Say(("Getting news for hash=" +hash).c_str());
    ResultErrorPtr p = QueryRunner::getTracker().getNews(hash);
    return p;
}


bool qWorker::MySqlFsFile::_flushWrite() {
    switch(_fileClass) {
    case fs::TWO_WRITE:
	return _flushWriteDetach();
    case fs::COMBO:
	return _flushWriteSync();
    default:
	_eDest->Say("Wrong filestate for writing. FIX THIS BUG.");
	_queryBuffer.reset(); // Kill the buffer.
	return false;
    }
    // switch should have already returned.
}

bool qWorker::MySqlFsFile::_flushWriteDetach() {
    qWorker::QueryRunnerArg a(_eDest, _userName, 
			      ScriptMeta(_queryBuffer, _chunkId));
    return flushOrQueue(a);
}

bool qWorker::MySqlFsFile::_flushWriteSync() {
    ScriptMeta s(_queryBuffer, _chunkId);
    _script = s.script;
    _setDumpNameAsChunkId(); // Because reads may get detached from writes.
    //_eDest->Say((Pformat("db=%1%.") % s.dbName).str().c_str());
    QueryRunner runner(*_eDest, _userName, s, _dumpName);
    return runner();
}

bool qWorker::MySqlFsFile::_hasPacketEof(
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



void qWorker::MySqlFsFile::_setDumpNameAsChunkId() {
    // This can get deprecated soon.
    std::stringstream ss;
    ss << DUMP_BASE << _chunkId << ".dump";
    ss >> _dumpName;
}

int qWorker::MySqlFsFile::_handleTwoReadOpen(char const* fileName) {
    std::string hash = fs::stripPath(fileName);
    _dumpName = hashToResultPath(hash); 
    _hasRead = false;
    ResultErrorPtr p = _getResultState(_dumpName);
    if(p.get()) {
	if(p->first == 0) {
	    _eDest->Say((Pformat("File open %1% for result reading by %2%")
			 % fileName % _userName).str().c_str());
	} else { // Error, so report it.
	    _eDest->Say((Pformat("File open %1% fail. Query error: %2%.")
			 % fileName % p->second).str().c_str());
            error.setErrInfo(EINVAL, p->second.c_str());
	    return SFS_ERROR;
	}
    } else {
	_addCallback(hash);
	return SFS_STARTED;
    }
    return SFS_OK;
}
