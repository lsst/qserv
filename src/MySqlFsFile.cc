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

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <functional>
#include <errno.h>
#include "mysql/mysql.h"
#include <numeric>
#ifdef __SUNPRO_CC
#include <sys/md5.h>
#else // Linux?
#include <openssl/md5.h>
#endif
#include <unistd.h>
#include <sstream>
#include <iostream> // For file-scoped debug output

#define QSERV_USE_STUPID_STRING 1

namespace qWorker = lsst::qserv::worker;

// Must end in a slash.
static std::string DUMP_BASE = "/tmp/qserv/";

static std::string CREATE_SUBCHUNK_SCRIPT =
    "CREATE DATABASE IF NOT EXISTS Subchunks_%1%;"
    "CREATE TABLE IF NOT EXISTS Subchunks_%1%.Object_%1%_%2% ENGINE = MEMORY "
    "AS SELECT * FROM LSST.Object_%1% WHERE subchunkId = %2%;";
static std::string CLEANUP_SUBCHUNK_SCRIPT =
    "DROP TABLE Subchunks_%1%.Object_%1%_%2%;";

class DbHandle {
public:
    DbHandle(void) : _db(mysql_init(0)) { };
    ~DbHandle(void) {
        if (_db) {
            mysql_close(_db);
            _db = 0;
        }
    };
    MYSQL* get(void) const { return _db; };
private:
    MYSQL* _db;
};

#if DO_NOT_USE_BOOST
typedef lsst::qserv::worker::PosFormat Pformat;
#else  
typedef boost::format Pformat;
#endif

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

template <class T> struct ptrDestroy {
    void operator() (T& x) { delete[] x.buffer;}
};

template <class T> struct offsetLess {
    bool operator() (T const& x, T const& y) { return x.offset < y.offset;}
};

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

static std::string hashQuery(char const* buffer, int bufferSize) {
    unsigned char hashVal[MD5_DIGEST_LENGTH];
    MD5(reinterpret_cast<unsigned char const*>(buffer), bufferSize, hashVal);
#ifdef DO_NOT_USE_BOOST
    return qWorker::hashFormat(hashVal, MD5_DIGEST_LENGTH);
#else
    std::string result;
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        result += (boost::format("%02x") % static_cast<int>(hashVal[i])).str();
    }
    return result;
#endif
}


static std::string hashToPath(std::string const& hash) {
    return DUMP_BASE +
        hash.substr(0, 3) + "/" + hash.substr(3, 3) + "/" + hash + ".dump";
}

static std::string runQuery(MYSQL* db, std::string query,
                            std::string arg=std::string()) {
    if (arg.size() != 0) {
        // TODO -- bind arg
    }
    if (mysql_real_query(db, query.c_str(), query.size()) != 0) {
        return std::string("Unable to execute query: ") + mysql_error(db) +
            "\nQuery = " + query;
    }
    int status = 0;
    do {
        MYSQL_RES* result = mysql_store_result(db);
        if (result) {
            // TODO -- Do something with it?
            mysql_free_result(result);
        }
        else if (mysql_field_count(db) != 0) {
            return "Unable to store result for query: " + query;
        }
        status = mysql_next_result(db);
        if (status > 0) {
            return std::string("Error retrieving results for query: ") +
                mysql_error(db) + "\nQuery = " + query;
        }
    } while (status == 0);
    return std::string();
}

static std::string runQueryInPieces(MYSQL* db, std::string query,
				    std::string arg=std::string()) {
    // Run a larger query in pieces split by semicolon/newlines.
    // This tries to avoid the max_allowed_packet
    // (MySQL client/server protocol) problem.
    // MySQL default max_allowed_packet=1MB
    std::string queryPiece;
    std::string subResult;
    std::string delimiter = ";\n";
    std::string::size_type pieceBegin=0;
    std::string::size_type pieceEnd=0;
    std::string::size_type qEnd=query.length();
    std::string::size_type sizeTarget=25;
    std::string::size_type searchTarget;
    unsigned pieceCount = 0;

    while(pieceEnd != qEnd) { 
	searchTarget = pieceBegin + sizeTarget;
	if(searchTarget < qEnd) {  // Is it worth splitting?
	    pieceEnd = query.rfind(delimiter, searchTarget);
	
	    // Did we find a split-point?
	    if((pieceEnd > pieceBegin) && (pieceEnd != std::string::npos)) {
		pieceEnd += delimiter.size();
	    } else {
		// Look forward instead of backward.
		pieceEnd = query.find(delimiter, pieceBegin + sizeTarget);
		if(pieceEnd != std::string::npos) { // Found?
		    pieceEnd += delimiter.size(); 
		} else { // Not found bkwd/fwd. Use end.
		    pieceEnd = qEnd; 
		}
	    }
	} else { // Remaining is small. Don't split further.
	    pieceEnd = qEnd; 
	}
	queryPiece = "";
	queryPiece.assign(query, pieceBegin, pieceEnd-pieceBegin);
	// Catch empty strings.
	if(!queryPiece.empty() && queryPiece[0] != '\0') {
	   subResult = runQuery(db, queryPiece, arg);
	  }
	// On error, the partial error is as good as the global.
	if(!subResult.empty()) {
	    unsigned s=pieceEnd-pieceBegin;
	    std::stringstream ss;
	    return subResult + (Pformat("---Error with piece %1% complete (size=%2%).") % pieceCount % s).str();;
	}
	++pieceCount;
	std::cout << Pformat("Piece %1% complete.") % pieceCount;

	pieceBegin = pieceEnd;
    }
    // Can't use _eDest (we are in file-scope)
    std::cout << Pformat("Executed query in %1% pieces.") % pieceCount;
    
    // Getting here means that none of the pieces failed.
    return std::string();
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

qWorker::MySqlFsFile::MySqlFsFile(XrdSysError* lp, char* user) :
    XrdSfsFile(user), _eDest(lp), 
    _socketFilename("/var/lib/mysql/mysql.sock"),
    _mysqldumpPath("/usr/bin/mysqldump") {

    // Capture userName at this point.
    // Param user is: user.pid:fd@host 
    // (See XRootd Protocol spec: 4.2.1.1 Connection name format)
    char* cursor = user;
    while(cursor && (*cursor != '.')) cursor++;
    _userName = std::string(user, cursor - user);
    // Try to capture socket filename from environment
    char* sock = ::getenv("QSW_DBSOCK");
    if(sock != (char*)0) { _socketFilename = sock; }
    // Capture alternative mysqldump
    char* path = ::getenv("QSW_MYSQLDUMP");
    if(path != (char*)0) { _mysqldumpPath = path; }
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

    _chunkId = findChunkNumber(fileName);
    _eDest->Say((Pformat("File open %1%(%2%) by %3%")
                 % fileName % _chunkId % _userName).str().c_str());
    return SFS_OK;
}

int qWorker::MySqlFsFile::close(void) {
    _eDest->Say((Pformat("File close(%1%) by %2%")
                 % _chunkId % _userName).str().c_str());
    
    // Must remove dump file while we are doing the single-query workaround
    int result = ::unlink(_dumpName.c_str());
    if(result != 0) {
	_eDest->Say((Pformat("Error removing dump file(%1%): %2%")
		     % _dumpName % strerror(errno)).str().c_str());
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

int dumpFileOpen(std::string const& dbName) {
    return open(dbName.c_str(), O_RDONLY);
}

bool dumpFileExists(std::string const& dbName) {
    struct stat statbuf;
    return ::stat(dbName.c_str(), &statbuf) == 0 &&
        S_ISREG(statbuf.st_mode) && (statbuf.st_mode & S_IRUSR) == S_IRUSR;
}

int qWorker::MySqlFsFile::read(XrdSfsFileOffset fileOffset,
                          XrdSfsXferSize prereadSz) {
    _eDest->Say((Pformat("File read(%1%) at %2% by %3%")
                 % _chunkId % fileOffset % _userName).str().c_str());
    if(_dumpName.empty()) { _setDumpNameAsChunkId(); }
    if (!dumpFileExists(_dumpName)) {
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
    msg = (Pformat("File read(%1%) at %2% for %3% by %4% [actual=%5%]")
	   % _chunkId % fileOffset % bufferSize % _userName % _dumpName).str();
    _eDest->Say(msg.c_str());
    if(_dumpName.empty()) { _setDumpNameAsChunkId(); }
    int fd = dumpFileOpen(_dumpName);
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
    // Spawn a throwaway thread that calls the normal, blocking read.
#if DO_NOT_USE_BOOST 
    ThreadDetail* td = newDetail<ReadCallable>(ReadCallable(*this, aioparm));
    ThreadManager::takeControl(td);
    Thread t(td);
#else
    boost::thread t(ReadCallable(*this, aioparm));
#endif
    return SFS_OK;
}

XrdSfsXferSize qWorker::MySqlFsFile::write(
    XrdSfsFileOffset fileOffset, char const* buffer,
    XrdSfsXferSize bufferSize) {
    _eDest->Say((Pformat("File write(%1%) at %2% for %3% by %4%")
                 % _chunkId % fileOffset % bufferSize % _userName).str().c_str());
    // for now, disable expected offset since writes can come out-of-order
    if (false) { 

    XrdSfsFileOffset expectedOffset = _queryBuffer.getLength();
    if (fileOffset != expectedOffset) {
	std::string msg = (Pformat("%1% Expected offset: %2%, got %3%.") 
			   % "Non-contiguous write to file." 
			   % expectedOffset % fileOffset).str();
        error.setErrInfo(EINVAL, msg.c_str());
    _eDest->Say((Pformat("File write(%1%) at %2% for %3% by %4% is broken: %5%")
                 % _chunkId % fileOffset % bufferSize % _userName % msg).str().c_str());

        //return -1;
	return -EINVAL;
    }
    }
    if (bufferSize <= 0) {
        error.setErrInfo(EINVAL, "No query provided");
        //return -1;
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
    _eDest->Say((Pformat("File write(%1%) at %2% for %3% by %4%  --FINISH--")
                 % _chunkId % fileOffset % bufferSize % _userName).str().c_str());
    
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
#if DO_NOT_USE_BOOST
    ThreadDetail* td = newDetail<WriteCallable>(WriteCallable(*this, aioparm, buffer));
    ThreadManager::takeControl(td);
    Thread t(td);
#else
    boost::thread t(WriteCallable(*this, aioparm, buffer));
#endif
    
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

void qWorker::MySqlFsFile::StringBuffer::addBuffer(
    XrdSfsFileOffset offset, char const* buffer, XrdSfsXferSize bufferSize) {
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
	_buffers.push_back(Fragment(offset, buffer, bufferSize));
	_totalSize += bufferSize;
    }
#endif
}

std::string qWorker::MySqlFsFile::StringBuffer::getStr() const {
#if QSERV_USE_STUPID_STRING
    // Cast away const in order to lock.
#if DO_NOT_USE_BOOST
    UniqueLock lock(const_cast<XrdSysMutex&>(_mutex));
#else
    boost::mutex& mutex = const_cast<boost::mutex&>(_mutex);
    boost::unique_lock<boost::mutex> lock(mutex);
#endif
    return _ss.str();
#else
    std::string accumulated;
    if(false) {
    // Cast away const to perform a sort (which doesn't logically change state)
    FragmentDeque& nonConst = const_cast<FragmentDeque&>(_buffers);
    std::sort(nonConst.begin(), nonConst.end(), offsetLess<Fragment>());
    }
    FragmentDeque::const_iterator bi; 
    FragmentDeque::const_iterator bend = _buffers.end(); 

    //    accumulated.assign(getLength(), '\0'); // 
    for(bi = _buffers.begin(); bi != bend; ++bi) {
	Fragment const& p = *bi;
	accumulated += std::string(p.buffer, p.bufferSize);
	// Perform "writes" of the buffers into the string
	// Assume that we end up with a contiguous string.
	//accumulated.replace(p.offset, p.bufferSize, p.buffer, p.bufferSize);
    }
    return accumulated;
#endif
}

std::string qWorker::MySqlFsFile::StringBuffer::getDigest() const {  
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


XrdSfsFileOffset qWorker::MySqlFsFile::StringBuffer::getLength() const {
    return _totalSize;
    // Might be wise to do a sanity check sometime (overlapping writes!)
#if 0
    struct accumulateSize {    
	XrdSfsXferSize operator() (XrdSfsFileOffset x, Fragment const& p) { 
	    return x + p.bufferSize; 
	}
    };
    return std::accumulate(_buffers.begin(), _buffers.end(), 
			   0, accumulateSize());
#endif
}


void qWorker::MySqlFsFile::StringBuffer::reset() {
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


bool qWorker::MySqlFsFile::_addWritePacket(XrdSfsFileOffset offset, 
					   char const* buffer, 
					   XrdSfsXferSize bufferSize) {
    _queryBuffer.addBuffer(offset, buffer, bufferSize);
    return true;
}

bool qWorker::MySqlFsFile::_flushWrite() {
    std::string digest =  _queryBuffer.getDigest();
    _eDest->Say("Getting digest");
    _eDest->Say(digest.c_str());
    std::string script = _queryBuffer.getStr();
    std::string hash = hashQuery(script.data(), script.length());
    // _dumpName = hashToPath(hash);
    // workaround _dumpName by forcing _dumpName = chunkname
    _setDumpNameAsChunkId();
    std::string dbName = "q_" + hash;
    // Do not print query-- could be multi-megabytes.
    _eDest->Say((Pformat("(fileobj:%3%) Db = %1%, dump = %2%")
                 % dbName % _dumpName % (void*)(this)).str().c_str());
    _eDest->Say(_queryBuffer.getDigest().c_str());

    if (dumpFileExists(_dumpName)) {
    _eDest->Say((Pformat("Reusing pre-existing dump = %1%")
                 % _dumpName).str().c_str());
        return true;
    }

    if (!_runScript(script, dbName)) {
	_eDest->Say((Pformat("(FinishFail:%3%) Db = %1%, dump = %2%")
                 % dbName % _dumpName % (void*)(this)).str().c_str());
	
        return false;
    } else {
	_eDest->Say((Pformat("(FinishOK:%3%) Db = %1%, dump = %2%")
                 % dbName % _dumpName % (void*)(this)).str().c_str());
    }
    return true;
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

std::string qWorker::MySqlFsFile::_runScriptPiece(
    MYSQL*const db,
    std::string const& scriptId, 
    std::string const& pieceName, std::string const& piece) {
    std::string result;
    _eDest->Say((Pformat("TIMING,%1%%2%Start,%3%")
                 % scriptId % pieceName % ::time(NULL)).str().c_str());
    result = runQueryInPieces(db, piece);
    _eDest->Say((Pformat("TIMING,%1%%2%Finish,%3%")
                 % scriptId % pieceName % ::time(NULL)).str().c_str());
    if(!result.empty()) {
	_eDest->Say((Pformat("Broken! ,%1%%2%---%3%")
		     % scriptId % pieceName % result).str().c_str());
	result += "(during " + pieceName + ")\nQueryFragment: " + piece;
    }
    return result;
}		   

std::string qWorker::MySqlFsFile::_runScriptPieces(
    MYSQL*const db,
    std::string const& scriptId, std::string const& build, 
    std::string const& run, std::string const& cleanup) {

    std::string result;    

    result = _runScriptPiece(db, scriptId, "QueryBuildSub", build);
    if(result.empty()) {
	result = _runScriptPiece(db, scriptId, "QueryExec", run);
	if(result.empty()) {
	}
	// Always destroy subchunks.
	result += _runScriptPiece(db, scriptId, "QueryDestroySub", cleanup);
    } 
    return result;
}


bool qWorker::MySqlFsFile::_runScript(
    std::string const& script, std::string const& dbName) {
    DbHandle db;
    std::string scriptId = dbName.substr(0, 6);
    _eDest->Say((Pformat("TIMING,%1%ScriptStart,%2%")
                 % scriptId % ::time(NULL)).str().c_str());

    if (mysql_real_connect(db.get(), 0, _userName.c_str(), 0, 0, 0, 
			   _socketFilename.c_str(),
                           CLIENT_MULTI_STATEMENTS) == 0) {
        error.setErrInfo(
            EIO, ("Unable to connect to MySQL as " + _userName).c_str());
        return false;
    }

    std::string result =
        runQuery(db.get(), "DROP DATABASE IF EXISTS " + dbName);
    if (result.size() != 0) {
        error.setErrInfo(EIO, result.c_str());
        return false;
    }

    result = runQuery(db.get(), "CREATE DATABASE " + dbName);
    if (result.size() != 0) {
        error.setErrInfo(EIO, result.c_str());
        return false;
    }

    if (mysql_select_db(db.get(), dbName.c_str()) != 0) {
        error.setErrInfo(EIO, ("Unable to select database " + dbName).c_str());
        return false;
    }

    std::string firstLine = script.substr(0, script.find('\n'));
    std::string buildScript;
    std::string cleanupScript;
    _eDest->Say((Pformat("TIMING,%1%QueryFormatStart,%2%")
                 % scriptId % ::time(NULL)).str().c_str());
    
#ifdef DO_NOT_USE_BOOST
    Regex re("[0-9][0-9]*");
    for(Regex::Iterator i = re.newIterator(firstLine);
	i != Regex::Iterator::end(); ++i) {
#else // }
    boost::regex re("\\d+");
    for (boost::sregex_iterator i = boost::make_regex_iterator(firstLine, re);
         i != boost::sregex_iterator(); ++i) {
#endif
	std::string subChunk = (*i).str(0);
        buildScript +=
            (Pformat(CREATE_SUBCHUNK_SCRIPT)
             % _chunkId % subChunk).str() + "\n";
        cleanupScript +=
            (Pformat(CLEANUP_SUBCHUNK_SCRIPT)
             % _chunkId % subChunk).str() + "\n";
    }
    _eDest->Say((Pformat("TIMING,%1%QueryFormatFinish,%2%")
                 % scriptId % ::time(NULL)).str().c_str());
    
    result = _runScriptPieces(db.get(), scriptId, buildScript, 
			      script, cleanupScript);
    if(!result.empty()) {
        error.setErrInfo(EIO, result.c_str());
	return false;
    }


    // mysqldump _dbName to _dumpName
    std::string::size_type pos = 0;
    struct stat statbuf;
    while ((pos = _dumpName.find('/', pos + 1)) != std::string::npos) {
        std::string dir(_dumpName, 0, pos);
        if (::stat(dir.c_str(), &statbuf) == -1) {
            if (errno == ENOENT) {
                mkdir(dir.c_str(), 0777);
            }
        }
    }

    std::string cmd = _mysqldumpPath + (Pformat(
            " --compact --add-locks --create-options --skip-lock-tables"
            " --result-file=%1% %2%")
                       % _dumpName % dbName).str();
    { 
	_eDest->Say((Pformat("TIMING,%1%QueryDumpStart,%2%")
		     % scriptId % ::time(NULL)).str().c_str());
	int cmdResult = system(cmd.c_str());
	_eDest->Say((Pformat("TIMING,%1%QueryDumpFinish,%2%")
		     % scriptId % ::time(NULL)).str().c_str());
	if (cmdResult != 0) {
	    error.setErrInfo(errno, ("Unable to dump database " + dbName +
                                 " to " + _dumpName).c_str());
	    return false;
	}
    }

    // Record query in query cache table
    /*
    result = runQuery(db.get(),
                      "INSERT INTO qcache.Queries "
                      "(queryTime, query, db, path) "
                      "VALUES (NOW(), ?, "
                          "'" + dbName + "'"
                          ", "
                          "'" + _dumpName + "'"
                          ")",
                      script);
    if (result.size() != 0) {
        error.setErrInfo(EIO, result.c_str());
        return false;
    }
    */

    result = runQuery(db.get(), "DROP DATABASE " + dbName);
    if (result.size() != 0) {
        error.setErrInfo(EIO, result.c_str());
        return false;
    }

    _eDest->Say((Pformat("TIMING,%1%ScriptFinish,%2%")
                 % scriptId % ::time(NULL)).str().c_str());

    return true;
}

void qWorker::MySqlFsFile::_setDumpNameAsChunkId() {
    std::stringstream ss;
    ss << DUMP_BASE << _chunkId << ".dump";
    ss >> _dumpName;
}
