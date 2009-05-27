#include "lsst/qserv/worker/MySqlFsFile.h"

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysError.hh"

#include "boost/regex.hpp"
#include "boost/format.hpp"
#include <errno.h>
#include "mysql/mysql.h"
#include <openssl/md5.h>

namespace qWorker = lsst::qserv::worker;

// Must end in a slash.
static std::string DUMP_BASE = "/tmp/qserv/";

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

static std::string hashQuery(char const* buffer, int bufferSize) {
    unsigned char hashVal[MD5_DIGEST_LENGTH];
    MD5(reinterpret_cast<unsigned char const*>(buffer), bufferSize, hashVal);
    std::string result;
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        result += (boost::format("%02x") % static_cast<int>(hashVal[i])).str();
    }
    return result;
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
        return "Unable to execute query: " + query;
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
            return "Error retrieving results for query: " + query;
        }
    } while (status == 0);
    return std::string();
}


qWorker::MySqlFsFile::MySqlFsFile(XrdSysError* lp, char* user) :
    XrdSfsFile(user), _eDest(lp), 
    _socketFilename("/var/lib/mysql/mysql.sock") {
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
    _chunkId = strtol(fileName, 0, 10);
    _eDest->Say((boost::format("File open(%1%) by %2%")
                 % _chunkId % _userName).str().c_str());
    return SFS_OK;
}

int qWorker::MySqlFsFile::close(void) {
    // optionally remove dump file
    _eDest->Say((boost::format("File close(%1%) by %2%")
                 % _chunkId % _userName).str().c_str());
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
    _eDest->Say((boost::format("File FName(%1%) by %2%")
                 % _chunkId % _userName).str().c_str());
    return 0;
}

int qWorker::MySqlFsFile::getMmap(void** Addr, off_t &Size) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}


int dumpFileOpen(std::string const& dbName) {
    return -1;
}

bool dumpFileExists(std::string const& dbName) {
    return false;
}

int qWorker::MySqlFsFile::read(XrdSfsFileOffset fileOffset,
                          XrdSfsXferSize prereadSz) {
    _eDest->Say((boost::format("File read(%1%) at %2% by %3%")
                 % _chunkId % fileOffset % _userName).str().c_str());
    if (!dumpFileExists(_dumpName)) {
        error.setErrInfo(ENOENT, "Query results missing");
        return SFS_ERROR;
    }
    return SFS_OK;
}

XrdSfsXferSize qWorker::MySqlFsFile::read(
    XrdSfsFileOffset fileOffset, char* buffer, XrdSfsXferSize bufferSize) {
    _eDest->Say((boost::format("File read(%1%) at %2% for %3% by %4%")
                 % _chunkId % fileOffset % bufferSize % _userName).str().c_str());
    int fd = dumpFileOpen(_dumpName);
    if (fd == -1) {
        error.setErrInfo(errno, "Query results missing");
        return -1;
    }
    off_t pos = lseek(fd, fileOffset, SEEK_SET);
    if (pos == static_cast<off_t>(-1) || pos != fileOffset) {
        error.setErrInfo(errno, "Unable to seek in query results");
        return -1;
    }
    ssize_t bytes = read(fd, buffer, bufferSize);
    if (bytes == -1) {
        error.setErrInfo(errno, "Unable to read query results");
        return -1;
    }
    return bytes;
}

int qWorker::MySqlFsFile::read(XrdSfsAio* aioparm) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

XrdSfsXferSize qWorker::MySqlFsFile::write(
    XrdSfsFileOffset fileOffset, char const* buffer,
    XrdSfsXferSize bufferSize) {
    _eDest->Say((boost::format("File write(%1%) at %2% for %3% by %4%")
                 % _chunkId % fileOffset % bufferSize % _userName).str().c_str());
    if (fileOffset != 0) {
        error.setErrInfo(EINVAL, "Write beyond beginning of file");
        return -1;
    }
    if (bufferSize <= 0) {
        error.setErrInfo(EINVAL, "No query provided");
        return -1;
    }

    std::string hash = hashQuery(buffer, bufferSize);
    _dumpName = hashToPath(hash);
    std::string dbName = "q_" + hash;

    if (dumpFileExists(_dumpName)) {
        return bufferSize;
    }

    _eDest->Say((boost::format("Db = %1%, dump = %2%:\n%3%")
                 % dbName % _dumpName % buffer).str().c_str());
    if (!_runScript(std::string(buffer, bufferSize), dbName)) {
        return -1;
    }
    return bufferSize;
}

int qWorker::MySqlFsFile::write(XrdSfsAio* aioparm) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
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


bool qWorker::MySqlFsFile::_runScript(
    std::string const& script, std::string const& dbName) {
    DbHandle db;
    if (mysql_real_connect(db.get(), 0, _userName.c_str(), 0, 0, 0, 
			   _socketFilename.c_str(),
                           CLIENT_MULTI_STATEMENTS) == 0) {
        error.setErrInfo(
            EIO, ("Unable to connect to MySQL as " + _userName).c_str());
        return false;
    }

    std::string result = runQuery(db.get(), "CREATE DATABASE " + dbName);
    if (result.size() != 0) {
        error.setErrInfo(EIO, result.c_str());
        return false;
    }

    if (mysql_select_db(db.get(), dbName.c_str()) != 0) {
        error.setErrInfo(EIO, ("Unable to select database " + dbName).c_str());
        return false;
    }

    boost::regex re("\\d+");
    std::string firstLine = script.substr(0, script.find('\n'));
    boost::sregex_iterator i = boost::make_regex_iterator(firstLine, re);
    while (i != boost::sregex_iterator()) {
        std::string subChunk = (*i).str(0);
        std::string processedQuery = (boost::format(script) % subChunk).str();
        result = runQuery(db.get(), processedQuery);
        if (result.size() != 0) {
            error.setErrInfo(
                EIO, (result + "\nQuery: " + processedQuery).c_str());
            return false;
        }
    }

    // mysqldump _dbName to _dumpName
    std::string cmd = (boost::format("mysqldump %1% > %2%")
                       % dbName % _dumpName).str();
    if (system(cmd.c_str()) != 0) {
        error.setErrInfo(errno, ("Unable to dump database " + dbName +
                                 " to " + _dumpName).c_str());
        return false;
    }

    // Record query in query cache table
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

    result = runQuery(db.get(), "DROP DATABASE " + dbName);
    if (result.size() != 0) {
        error.setErrInfo(EIO, result.c_str());
        return false;
    }

    return true;
}
