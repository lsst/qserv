// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
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

#include "xrdfs/MySqlFs.h"

// System headers
#include <cassert>
#include <cerrno>
#include <iterator>
#include <iostream>

// Third-party headers
#include "XProtocol/XProtocol.hh"
#include "XrdOuc/XrdOucCallBack.hh" // For Open-callbacks(FinishListener)
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysError.hh"

#include "obsolete/QservPath.h"
#include "sql/SqlConnection.h"
#include "wconfig/Config.h"
#include "wcontrol/Service.h"
#include "wdb/QueryRunner.h"
#include "wlog/WLogger.h"
#include "wpublish/ChunkInventory.h"
#include "xrdfs/MySqlFsDirectory.h"
#include "xrdfs/MySqlFsFile.h"
#include "xrdfs/XrdName.h"
#include "xrdfs/XrdPrinter.h"



// Externally declare XrdSfs loader to cheat on Andy's suggestion.
#if 1
extern XrdSfsFileSystem*
XrdSfsGetDefaultFileSystem(XrdSfsFileSystem* nativeFS,
                           XrdSysLogger* sysLogger,
                           const char* configFn,
                           XrdOucEnv* envInfo);

#else
XrdSfsFileSystem*
XrdSfsGetDefaultFileSystem(XrdSfsFileSystem* nativeFS,
                           XrdSysLogger* sysLogger,
                           const char* configFn) {
    return 0;
}
#endif

namespace {

#ifdef NO_XROOTD_FS // Fake placeholder functions
class FakeAddCallback : public lsst::qserv::xrdfs::AddCallbackFunction {
public:
    typedef boost::shared_ptr<FakeAddCallback> Ptr;
    virtual ~FakeAddCallback() {}
    virtual void operator()(XrdSfsFile& caller, std::string const& filename) {
    }
};

class FakeFileValidator : public lsst::qserv::xrdfs::FileValidator {
public:
    typedef boost::shared_ptr<FakeFileValidator> Ptr;
    FakeFileValidator() {}
    virtual ~FakeFileValidator() {}
    virtual bool operator()(std::string const& filename) {
        return true;
    }
};

#else // "Real" helper functors
template<class Callback>
class FinishListener {
public:
    FinishListener(Callback* cb) : _callback(cb) {}
    virtual void operator()(lsst::qserv::wcontrol::ResultError const& p) {
        if(p.first == 0) {
            // std::cerr << "Callback=OK!\t" << (void*)_callback << std::endl;
            //_callback->Reply_OK();
            _callback->Reply(SFS_OK, 0, 0);
        } else {
            //std::cerr << "Callback error! " << p.first
            //	      << " desc=" << p.second << std::endl;
            //_callback->Reply_Error(p.first, p.second.c_str());
            int code = 0;
            switch(p.first) {
            case -1: code = kXR_FSError; break;
            case -2: code = kXR_NoMemory; break;
            default: code = kXR_NotAuthorized; break;
            };
            _callback->Reply(SFS_ERROR, code, p.second.c_str());
        }
        _callback = 0;
        // _callback will be auto-destructed after any Reply_* call.
    }
private:
    Callback* _callback;
};

/// An AddCallbackFunction implementation to provide xrootd-backed callbacks.
class AddCallbackFunc : public lsst::qserv::xrdfs::AddCallbackFunction {
public:
    typedef boost::shared_ptr<AddCallbackFunc> Ptr;
    virtual ~AddCallbackFunc() {}
    virtual void operator()(XrdSfsFile& caller, std::string const& filename) {
        XrdOucCallBack * callback = _createCallback(&(caller.error));
        //XrdOucCallBack * callback = new XrdOucCallBack();
        //callback->Init(&(caller.error));

        // Register callback with opener.
        //std::cerr << "Callback reg!\t" << (void*)callback << std::endl;
        lsst::qserv::wdb::QueryRunner::getTracker().listenOnce(
            filename, FinishListener<XrdOucCallBack>(callback));
        // wdb::QueryRunner::getTracker().listenOnce(
        //     filename, FinishListener<XrdOucCallBack>(callback));
    }
private:
    XrdOucCallBack* _createCallback(XrdOucErrInfo* eInfo) {
        // Trying to use approach from XrdOfsTPCInfo
        XrdOucCallBack* newCB;
        newCB = new XrdOucCallBack();
        if(!newCB->Init(eInfo)) {
            delete newCB;
            newCB = 0;
            throw std::logic_error("Bug in callback creation. Call Andy.");
        }
        return newCB;
    }
};
#endif // ifndef NO_XROOTD_FS

/// ChunkValidator
/// Uses exports data struct instead of hitting the filesystem.
class ChunkValidator : public lsst::qserv::xrdfs::FileValidator {
public:
    typedef lsst::qserv::wpublish::ChunkInventory ChunkInventory;
    typedef boost::shared_ptr<ChunkValidator> Ptr;
    ChunkValidator(ChunkInventory::CPtr c)
        : _chunkInventory(c) {}
    virtual ~ChunkValidator() {}
    virtual bool operator()(std::string const& filename) {
        lsst::qserv::obsolete::QservPath qp(filename);
        if(qp.requestType() != lsst::qserv::obsolete::QservPath::CQUERY) {
            return false; // Don't validate non chunk-query paths now.
        }
        return _chunkInventory->has(qp.db(), qp.chunk());
    }
private:
    ChunkInventory::CPtr _chunkInventory;
};
} // anonymous namespace


namespace lsst {
namespace qserv {
namespace xrdfs {

class XrdfsConfigError : public std::exception {
public:
    XrdfsConfigError(std::string const& msg) : _msg(msg) {}
    virtual ~XrdfsConfigError() throw() {}
    virtual const char* what() const throw() {
        try {
            return _msg.c_str();
        } catch(...) {} // Silence any exceptions
        return "";
    }
    std::string _msg;
};

////////////////////////////////////////////////////////////////////////
// xrdfs-scope helpers
////////////////////////////////////////////////////////////////////////
boost::shared_ptr<sql::SqlConnection> makeSqlConnection() {
    boost::shared_ptr<sql::SqlConnection> conn;
    mysql::MySqlConfig sqlConfig = wconfig::getConfig().getSqlConfig();
    // FIXME: Use qsmaster privileges for now.
    sqlConfig.username = "qsmaster";
    sqlConfig.dbName = "";
    conn.reset(new sql::SqlConnection(sqlConfig, true));
    return conn;
}

////////////////////////////////////////////////////////////////////////
// class MySqlFs
////////////////////////////////////////////////////////////////////////
MySqlFs::MySqlFs(boost::shared_ptr<wlog::WLogger> log, XrdSysLogger* lp,
                 char const* cFileName)
    : XrdSfsFileSystem(), _log(log) {
    if(!wconfig::getConfig().getIsValid()) {
        std::string msg("Configuration invalid: "
                        + wconfig::getConfig().getError());
        throw XrdfsConfigError(msg);
    }
#ifdef NO_XROOTD_FS
    _log->info("Skipping load of libXrdOfs.so (non xrootd build).");
#else
    // Passing NULL XrdOucEnv*. The XrdOucEnv* parameter was new in xrootd 3.3.x
    XrdSfsFileSystem* fs;
    fs = XrdSfsGetDefaultFileSystem(0, lp, cFileName, 0);
    if(fs == 0) {
        _log->warn("Problem loading XrdSfsDefaultFileSystem. Clustering won't work.");
    }
#endif
    wbase::updateResultPath();
    wbase::clearResultPath();
    _localroot = ::getenv("XRDLCLROOT");
    if (!_localroot) {
        _log->warn("No XRDLCLROOT set. Bug in xrootd?");
        _localroot = "";
    }
    boost::shared_ptr<sql::SqlConnection> conn = makeSqlConnection();
    _initExports();
    assert(_chunkInventory);
    _cleanup();
    _service.reset(new wcontrol::Service(_log));
}

MySqlFs::~MySqlFs(void) {
    if(!_isMysqlFail) {
        mysql_library_end();
    }
}

// Object Allocation Functions
//
XrdSfsDirectory*
MySqlFs::newDir(char* user, int MonID) {
    return new MySqlFsDirectory(_log, user);
}

XrdSfsFile*
MySqlFs::newFile(char* user, int MonID) {
#ifdef NO_XROOTD_FS
    return new MySqlFsFile(
        _log, user,
        boost::make_shared<FakeAddCallback>(),
        boost::make_shared<FakeFileValidator>(),
        _service);
#else
    assert(_chunkInventory.get());
    return new MySqlFsFile(
        _log, user,
        boost::make_shared<AddCallbackFunc>(),
        boost::make_shared<ChunkValidator>(_chunkInventory), _service);
#endif
}

// Other Functions
//
int MySqlFs::chmod(
    char const* Name, XrdSfsMode Mode, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFs::exists(
    char const* fileName, XrdSfsFileExistence& existsFlag,
    XrdOucErrInfo& outError, XrdSecEntity const* client,
    char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFs::fsctl(
    int const cmd, char const* args, XrdOucErrInfo& outError,
    XrdSecEntity const* client) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFs::getStats(char* buff, int blen) {
    return SFS_ERROR;
}

char const* MySqlFs::getVersion(void) {
    return "$Id$"; // Eventually, use git describe
}

int MySqlFs::mkdir(
    char const* dirName, XrdSfsMode Mode, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFs::prepare(
    XrdSfsPrep& pargs, XrdOucErrInfo& outError, XrdSecEntity const* client) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

/// rem() : discard/squash a query result and the running/queued query
///  that would-have/has-had produced it.
int MySqlFs::rem(
    char const* path, XrdOucErrInfo& outError, XrdSecEntity const* client,
    char const* opaque) {
    // Check for qserv result path
    FileClass c = computeFileClass(path);
    if(c != TWO_READ) { // Only support removal of result files.
        outError.setErrInfo(ENOTSUP, "Operation not supported");
        return SFS_ERROR;
    }
    std::string hash = stripPath(path);
    // Signal query squashing
    _service->squashByHash(hash);
    //wdb::QueryRunner::Manager& mgr = wdb::QueryRunner::getMgr();
    //mgr.squashByHash(hash);
    return SFS_OK;
}

int MySqlFs::remdir(
    char const* dirName, XrdOucErrInfo& outError, XrdSecEntity const* client,
    char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFs::rename(
    char const* oldFileName, char const* newFileName, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaqueO, char const* opaqueN) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFs::stat(
    char const* Name, struct stat* buf, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFs::stat(
    char const* Name, mode_t& mode, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int MySqlFs::truncate(
    char const* Name, XrdSfsFileOffset fileOffset, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

////////////////////////////////////////////////////////////////////////
// MySqlFs private
////////////////////////////////////////////////////////////////////////
void MySqlFs::_initExports() {
    XrdName x;
    boost::shared_ptr<sql::SqlConnection> conn = makeSqlConnection();
    assert(conn);
    _chunkInventory.reset(new wpublish::ChunkInventory(x.getName(), *_log, conn));
    std::ostringstream os;
    os << "Paths exported: ";
    _chunkInventory->dbgPrint(os);
    _log->info(os.str());
}

/// Cleanup scratch space and scratch dbs.
/// This means that scratch db and scratch dirs CANNOT be shared among
/// qserv workers. Take heed.
/// @return true if cleanup was successful, false otherwise.
bool MySqlFs::_cleanup() {
    boost::shared_ptr<sql::SqlConnection> conn = makeSqlConnection();
    if(!conn) {
        return false;
    }
    sql::SqlErrorObject errObj;
    std::string dbName = wconfig::getConfig().getString("scratchDb");
    _log->info((Pformat("Cleaning up scratchDb: %1%.")
                % dbName).str());
    if(!conn->dropDb(dbName, errObj, false)) {
        _log->error((Pformat("Cfg error! couldn't drop scratchDb: %1% %2%.")
                     % dbName % errObj.errMsg()).str());
        return false;
    }
    errObj.reset();
    if(!conn->createDb(dbName, errObj, true)) {
        _log->error((Pformat("Cfg error! couldn't create scratchDb: %1% %2%.")
                     % dbName % errObj.errMsg()).str());
        return false;
    }
    return true;
}

}}} // namespace lsst::qserv::xrdfs

class XrdSysLogger;

extern "C" {

XrdSfsFileSystem* XrdSfsGetFileSystem(
    XrdSfsFileSystem* native_fs, XrdSysLogger* lp, char const* fileName) {
    static boost::shared_ptr<lsst::qserv::wlog::WLogger> log;
    boost::shared_ptr<lsst::qserv::wlog::WLogger::Printer>
        p(new lsst::qserv::xrdfs::XrdPrinter(lp));
    if(!log.get()) {
        log.reset(new lsst::qserv::wlog::WLogger(p));
    }
    static lsst::qserv::xrdfs::MySqlFs myFS(log, lp, fileName);

    log->info("MySqlFs (MySQL File System)");
    log->info(myFS.getVersion());
    return &myFS;
}

} // extern "C"
