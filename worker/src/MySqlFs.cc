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
 
#include "lsst/qserv/worker/MySqlFs.h"

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSfs/XrdSfsCallBack.hh" // For Open-callbacks(FinishListener)

#include "lsst/qserv/worker/MySqlFsDirectory.h"
#include "lsst/qserv/worker/MySqlFsFile.h"
#include "lsst/qserv/worker/MySqlExportMgr.h"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/worker/Service.h"
#include "lsst/qserv/worker/Logger.h"
#include "lsst/qserv/worker/XrdName.h"


#include "lsst/qserv/QservPath.hh"
#include <cerrno>
#include <iostream>

// Externally declare XrdSfs loader to cheat on Andy's suggestion.
extern XrdSfsFileSystem*
XrdSfsGetDefaultFileSystem(XrdSfsFileSystem* nativeFS,
                           XrdSysLogger* sysLogger,
                           const char* configFn);


namespace qWorker = lsst::qserv::worker;
using namespace lsst::qserv::worker;
using lsst::qserv::QservPath;

namespace { 

#ifdef NO_XROOTD_FS // Fake placeholder functions
class FakeAddCallback : public AddCallbackFunction {
public:
    typedef boost::shared_ptr<FakeAddCallback> Ptr;
    virtual ~FakeAddCallback() {}
    virtual void operator()(XrdSfsFile& caller, std::string const& filename) {
    }
};    

class FakeFileValidator : public fs::FileValidator {
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
    virtual void operator()(ResultError const& p) {
        if(p.first == 0) {
            // std::cerr << "Callback=OK!\t" << (void*)_callback << std::endl;
            _callback->Reply_OK();
        } else {
            //std::cerr << "Callback error! " << p.first 
            //	      << " desc=" << p.second << std::endl;
            _callback->Reply_Error(p.first, p.second.c_str());
        }
        _callback = 0;
        // _callback will be auto-destructed after any Reply_* call.
    }
private:
    Callback* _callback;
};

/// An AddCallbackFunction implementation to provide xrootd-backed callbacks.
class AddCallbackFunc : public AddCallbackFunction {
public:
    typedef boost::shared_ptr<AddCallbackFunc> Ptr;
    virtual ~AddCallbackFunc() {}
    virtual void operator()(XrdSfsFile& caller, std::string const& filename) {
        XrdSfsCallBack * callback = XrdSfsCallBack::Create(&(caller.error));
        // Register callback with opener.
        //std::cerr << "Callback reg!\t" << (void*)callback << std::endl;
        QueryRunner::getTracker().listenOnce(
                           filename, FinishListener<XrdSfsCallBack>(callback));
    }
};
#endif // ifndef NO_XROOTD_FS

/// Filesystem-based file path validation. Deprecated in favor of the
/// internal data-structure-backed PathValidator (populated via mysqld
/// upon startup/initialization)
class FileValidator : public fs::FileValidator {
public:
    typedef boost::shared_ptr<FileValidator> Ptr;
    FileValidator(char const* localroot) : _localroot(localroot) {}
    virtual ~FileValidator() {}
    virtual bool operator()(std::string const& filename) {
        std::string expanded(_localroot);
        expanded += "/" + filename;
        struct stat statbuf;
        return ::stat(expanded.c_str(), &statbuf) == 0 &&
            S_ISREG(statbuf.st_mode) && 
            (statbuf.st_mode & S_IRUSR) == S_IRUSR;
    }
private:
    char const* _localroot;
};

/// PathValidator
/// Uses exports data struct instead of hitting the filesystem.
class PathValidator : public fs::FileValidator {
public:
    typedef boost::shared_ptr<PathValidator> Ptr;
    PathValidator(MySqlExportMgr::StringSet const& exports)
        : _exports(exports) {}
    virtual ~PathValidator() {}
    virtual bool operator()(std::string const& filename) {
        QservPath qp(filename);
        if(qp.requestType() != QservPath::CQUERY) {
            return false; // Don't validate non chunk-query paths now.
        }
        return MySqlExportMgr::checkExist(_exports, qp.db(), qp.chunk());
    }
private:
    MySqlExportMgr::StringSet const& _exports;
};
} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// class MySqlFs
////////////////////////////////////////////////////////////////////////
MySqlFs::MySqlFs(boost::shared_ptr<Logger> log, XrdSysLogger* lp, 
                 char const* cFileName) 
    : XrdSfsFileSystem(), _log(log) {
    if(!getConfig().getIsValid()) {
        log->error(("Configration invalid: " + getConfig().getError() 
                     + " -- Behavior undefined.").c_str());
    }
#ifdef NO_XROOTD_FS
    _log->info("Skipping load of libXrdOfs.so (non xrootd build).");
#else
    XrdSfsFileSystem* fs;
    fs = XrdSfsGetDefaultFileSystem(0, lp, cFileName);
    if(fs == 0) {
        _log->warn("Problem loading XrdSfsDefaultFileSystem. Clustering won't work.");
    }
#endif
    updateResultPath();
    clearResultPath();
    _localroot = ::getenv("XRDLCLROOT");
    if (!_localroot) {
        _log->warn("No XRDLCLROOT set. Bug in xrootd?");
        _localroot = "";
    }   
    _initExports();
    assert(_exports.get());
    _service.reset(new Service(_log));
}

MySqlFs::~MySqlFs(void) {
    if(!_isMysqlFail) {
        mysql_library_end();
    }
}

// Object Allocation Functions
//
XrdSfsDirectory* MySqlFs::newDir(char* user) {
    return new MySqlFsDirectory(_log, user);
}

XrdSfsFile* MySqlFs::newFile(char* user) {
#ifdef NO_XROOTD_FS
    return new MySqlFsFile(
        _log, user, 
        boost::make_shared<FakeAddCallback>(),
        boost::make_shared<FakeFileValidator>(),
        _service);
#else
    assert(_exports.get());
    return new MySqlFsFile(
        _log, user, 
        boost::make_shared<AddCallbackFunc>(),
        boost::make_shared<PathValidator>(*_exports),
        _service);
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
    fs::FileClass c = fs::computeFileClass(path);
    if(c != fs::TWO_READ) { // Only support removal of result files.
        outError.setErrInfo(ENOTSUP, "Operation not supported");
        return SFS_ERROR;
    }
    std::string hash = fs::stripPath(path);
    // Signal query squashing
    QueryRunner::Manager& mgr = QueryRunner::getMgr();
    mgr.squashByHash(hash);
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
    _exports.reset(new StringSet);
    XrdName x;
    MySqlExportMgr m(x.getName(), *_log);
    m.fillDbChunks(*_exports);
}
class XrdSysLogger;

extern "C" {

XrdSfsFileSystem* XrdSfsGetFileSystem(
    XrdSfsFileSystem* native_fs, XrdSysLogger* lp, char const* fileName) {
    static boost::shared_ptr<Logger> log;
    if(!log.get()) {
        log.reset(new Logger(lp));
    }
    static MySqlFs myFS(log, lp, fileName);

    log->info("MySqlFs (MySQL File System)");
    log->info(myFS.getVersion());
    return &myFS;
}

} // extern "C"
