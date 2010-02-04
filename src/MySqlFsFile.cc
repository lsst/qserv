#include "lsst/qserv/worker/MySqlFsFile.h"

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSfs/XrdSfsCallBack.hh" // For Open-callbacks(FinishListener)
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
	if(_aioparm->Result != _sfsAio.aio_nbytes) {
	    // overwrite error result with generic IO error?
	    _aioparm->Result = -EIO;
	}
	_aioparm->doneWrite();
    }

private:
    qWorker::MySqlFsFile& _fsfile;
    XrdSfsAio* _aioparm;
    char* _buffer;
    struct aiocb& _sfsAio;
    static qWorker::Semaphore _sema;
};
// for now, two simultaneous writes (queries)
qWorker::Semaphore WriteCallable::_sema(2);




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

qWorker::MySqlFsFile::MySqlFsFile(XrdSysError* lp, char* user) :
    XrdSfsFile(user), _eDest(lp) {

    // Capture userName at this point.
    // Param user is: user.pid:fd@host 
    // (See XRootd Protocol spec: 4.2.1.1 Connection name format)
    char* cursor = user;
    while(cursor && (*cursor != '.')) cursor++;
    _userName = std::string(user, cursor - user);
}

qWorker::MySqlFsFile::~MySqlFsFile(void) {
}

int qWorker::MySqlFsFile::open(
    char const* fileName, XrdSfsFileOpenMode openMode, mode_t createMode,
    XrdSecEntity const* client, char const* opaque) {
    if (fileName == 0) {
        error.setErrInfo(EINVAL, "Null filename");
        return SFS_ERROR;
    }
    _fileClass = _getFileClass(fileName);
    switch(_fileClass) {
    case COMBO:
	_chunkId = findChunkNumber(fileName);
	_eDest->Say((Pformat("File open %1%(%2%) by %3%")
		     % fileName % _chunkId % _userName).str().c_str());
	break;
    case TWO_WRITE:
	_eDest->Say((Pformat("File open %1% for query invocation by %3%")
		     % fileName % _userName).str().c_str());
	break;
    case TWO_READ:
	_dumpName = hashToResultPath(fileName); 
	_hasRead = false;
	if(_isResultReady(fileName)) {
	    _eDest->Say((Pformat("File open %1% for result reading by %2%")
			 % fileName % _userName).str().c_str());
	} else {
	    _addCallback(_stripPath(fileName));
	    return SFS_STARTED;
	}

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
    if((_fileClass == COMBO)  ||
       ((_fileClass == TWO_READ) && _hasRead) )	{
	// Must remove dump file while we are doing the single-query workaround
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
      std::string s;
      ss >> s;
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
    std::string descr((Pformat("File write(%1%) at %2% for %3% by %4%")
		       % _chunkId % fileOffset % bufferSize % _userName).str());
    _eDest->Say(descr.c_str());

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
    return bufferSize;
}

	
int qWorker::MySqlFsFile::write(XrdSfsAio* aioparm) {
    // Spawn a thread that calls the normal write call.
    char* buffer = new char[aioparm->sfsAio.aio_nbytes];
    assert(buffer != (char*)0);
    memcpy(buffer, (char const*)aioparm->sfsAio.aio_buf, 
	   aioparm->sfsAio.aio_nbytes);
    int printlen = 100;
    if(printlen > aioparm->sfsAio.aio_nbytes) {
	printlen = aioparm->sfsAio.aio_nbytes;
    }
    std::string s(buffer,printlen);
    int offset = aioparm->sfsAio.aio_offset;
    _eDest->Say((Pformat("File write(%1%) at %2% : %3%")
                 % _chunkId % offset % s).str().c_str());
    launchThread(WriteCallable(*this, aioparm, buffer));
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

class FinishListener { // Inherit from a running object.
public:
    FinishListener(XrdSfsCallBack* cb) : _callback(cb) {}
    virtual void operator()(qWorker::ErrorPair const& p) {
	if(p.first != 0) {
	    _callback->Reply_OK();
	} else {
	    _callback->Reply_Error(p.first, p.second.c_str());
	}
	_callback = 0;
	// _callback will be auto-destructed after any Reply_* call.
    }
private:
    XrdSfsCallBack* _callback;
};

void qWorker::MySqlFsFile::_addCallback(std::string const& filename) {
    assert(_fileClass == TWO_READ);
    // Construct callback.
    XrdSfsCallBack * callback = XrdSfsCallBack::Create(&error);
    // Add callback to running object
    QueryRunner::getTracker().listenOnce(filename, FinishListener(callback));
    // FIXME
}

bool qWorker::MySqlFsFile::_isResultReady(char const* filename) {
    assert(_fileClass == TWO_READ);
    // Lookup result hash.
    ErrorPtr p = QueryRunner::getTracker().getNews(filename);
    // Check if query done.
    if(p.get() != 0) {
	return true;
	// Sanity check that result file exists.
	// FIXME
    }

    return false; 
}


bool qWorker::MySqlFsFile::_flushWrite() {
    switch(_fileClass) {
    case TWO_WRITE:
	return _flushWriteDetach();
    case COMBO:
	return _flushWriteSync();
    default:
	_eDest->Say("Wrong filestate for writing. FIX THIS BUG.");
	_queryBuffer.reset(); // Kill the buffer.
	return false;
    }
    // switch should have already returned.
}

bool qWorker::MySqlFsFile::_flushWriteDetach() {
    ScriptMeta s(_queryBuffer, _chunkId);
    _script = s.script;
    // Spawn.
    _eDest->Say((Pformat("Unattached exec in flight for Db = %1%, dump = %2%")
                 % s.dbName % s.resultPath % (void*)(this)).str().c_str());
    launchThread(QueryRunner(error, *_eDest, _userName, s));
    return true;
}

bool qWorker::MySqlFsFile::_flushWriteSync() {
    ScriptMeta s(_queryBuffer, _chunkId);
    _script = s.script;
    _setDumpNameAsChunkId(); // Because reads may get detached from writes.

    QueryRunner runner(error, *_eDest, _userName, s, _dumpName);
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

qWorker::MySqlFsFile::FileClass qWorker::MySqlFsFile::_getFileClass(std::string const& filename) {
    if(std::string::npos != filename.find("/query2/")) {
	return TWO_WRITE;
    } else if(std::string::npos != filename.find("/result/")) {
	return TWO_READ;
    } else if(std::string::npos != filename.find("/query/")) {
	return COMBO;
    } else {
	return UNKNOWN;
    }

}

std::string qWorker::MySqlFsFile::_stripPath(std::string const& filename) {
    // Expecting something like "/results/0123aeb31b1c29a"
    // Strip out everything before and including the last /
    std::string::size_type pos = filename.rfind("/");
    if(pos == std::string::npos) {
	return filename;
    }
    return filename.substr(1+pos, std::string::npos);
	
}


void qWorker::MySqlFsFile::_setDumpNameAsChunkId() {
    // This can get deprecated soon.
    std::stringstream ss;
    ss << DUMP_BASE << _chunkId << ".dump";
    ss >> _dumpName;
}
