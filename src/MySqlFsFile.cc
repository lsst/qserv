#include "lsst/qserv/worker/MySqlFsFile.h"

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysError.hh"

#include "boost/regex.hpp"
#include "boost/format.hpp"
#include <cstdlib>
#include <fcntl.h>
#include <errno.h>
#include "mysql/mysql.h"
#include <openssl/md5.h>
#include <unistd.h>

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

    _chunkId = findChunkNumber(fileName);
    _eDest->Say((boost::format("File open %1%(%2%) by %3%")
                 % fileName % _chunkId % _userName).str().c_str());
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
    return open(dbName.c_str(), O_RDONLY);
}

bool dumpFileExists(std::string const& dbName) {
    struct stat statbuf;
    return ::stat(dbName.c_str(), &statbuf) == 0 &&
        S_ISREG(statbuf.st_mode) && (statbuf.st_mode & S_IRUSR) == S_IRUSR;
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
    off_t pos = ::lseek(fd, fileOffset, SEEK_SET);
    if (pos == static_cast<off_t>(-1) || pos != fileOffset) {
        error.setErrInfo(errno, "Unable to seek in query results");
        return -1;
    }
    ssize_t bytes = ::read(fd, buffer, bufferSize);
    if (bytes == -1) {
        error.setErrInfo(errno, "Unable to read query results");
        return -1;
    }
    ::close(fd);
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

    std::string strBuffer(buffer, bufferSize);

    _eDest->Say((boost::format("Db = %1%, dump = %2%:\n%3%")
                 % dbName % _dumpName % strBuffer).str().c_str());
    if (!_runScript(strBuffer, dbName)) {
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
    boost::regex re("\\d+");
    std::string processedQuery;
    std::string cleanupScript;
    for (boost::sregex_iterator i = boost::make_regex_iterator(firstLine, re);
         i != boost::sregex_iterator(); ++i) {
        std::string subChunk = (*i).str(0);
        processedQuery +=
            (boost::format(CREATE_SUBCHUNK_SCRIPT)
             % _chunkId % subChunk).str();
        cleanupScript +=
            (boost::format(CLEANUP_SUBCHUNK_SCRIPT)
             % _chunkId % subChunk).str();
    }
    processedQuery += script;
    processedQuery += cleanupScript;
    result = runQuery(db.get(), processedQuery);
    if (result.size() != 0) {
        error.setErrInfo(EIO, (result + "\nQuery: " + processedQuery).c_str());
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

    std::string cmd = (boost::format(
            "/usr/bin/mysqldump"
            " --compact --add-locks --create-options --skip-lock-tables"
            " --result-file=%1% %2%")
                       % _dumpName % dbName).str();
    if (system(cmd.c_str()) != 0) {
        error.setErrInfo(errno, ("Unable to dump database " + dbName +
                                 " to " + _dumpName).c_str());
        return false;
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

    return true;
}
