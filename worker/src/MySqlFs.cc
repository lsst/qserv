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
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/worker/Service.h"


#include "lsst/qserv/QservPath.hh"
#include <cerrno>
#include <iostream>

// Externally declare XrdSfs loader to cheat on Andy's suggestion.
extern XrdSfsFileSystem *XrdXrootdloadFileSystem(XrdSysError *, char *, 
                                                 const char *);
namespace qWorker = lsst::qserv::worker;

namespace { 

#ifdef NO_XROOTD_FS // Fake placeholder functions
class FakeAddCallback : public qWorker::AddCallbackFunction {
public:
    typedef boost::shared_ptr<FakeAddCallback> Ptr;
    virtual ~FakeAddCallback() {}
    virtual void operator()(XrdSfsFile& caller, std::string const& filename) {
    }
};    

class FakeFileValidator : public qWorker::fs::FileValidator {
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
    virtual void operator()(qWorker::ResultError const& p) {
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

class AddCallbackFunc : public qWorker::AddCallbackFunction {
public:
    typedef boost::shared_ptr<AddCallbackFunc> Ptr;
    virtual ~AddCallbackFunc() {}
    virtual void operator()(XrdSfsFile& caller, std::string const& filename) {
        XrdSfsCallBack * callback = XrdSfsCallBack::Create(&(caller.error));
        // Register callback with opener.
        //std::cerr << "Callback reg!\t" << (void*)callback << std::endl;
        qWorker::QueryRunner::getTracker().listenOnce(
                           filename, FinishListener<XrdSfsCallBack>(callback));
    }
};
#endif // ifndef NO_XROOTD_FS

class FileValidator : public qWorker::fs::FileValidator {
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

} // anonymous namespace

qWorker::MySqlFs::MySqlFs(XrdSysError* lp, char const* cFileName) 
    : XrdSfsFileSystem(), _eDest(lp) {
    static boost::mutex m;
    boost::lock_guard<boost::mutex> l(m);
    _eDest->Say("MySqlFs initializing mysql library.");
    _isMysqlFail = mysql_library_init(0, NULL, NULL);
    if(_isMysqlFail) {
        _eDest->Say("Problem initializing MySQL library. Behavior undefined.");
    }
    if(!getConfig().getIsValid()) {
        _eDest->Say(("Configration invalid: " + getConfig().getError() 
                     + " -- Behavior undefined.").c_str());
    }
#ifdef NO_XROOTD_FS
    _eDest->Say("Skipping load of libXrdOfs.so (non xrootd build).");
#else
    XrdSfsFileSystem* fs;
    fs = XrdXrootdloadFileSystem(_eDest, 
                                 const_cast<char*>("libXrdOfs.so"), cFileName);
    if(fs == 0) {
        _eDest->Say("Problem loading libXrdOfs.so. Clustering won't work.");
    }
#endif
    updateResultPath();
    clearResultPath();
    _localroot = ::getenv("XRDLCLROOT");
    if (!_localroot) {
        _localroot = "";
    }
    _service.reset(new Service()); 
}

qWorker::MySqlFs::~MySqlFs(void) {
    if(!_isMysqlFail) {
        mysql_library_end();
    }
}

// Object Allocation Functions
//
XrdSfsDirectory* qWorker::MySqlFs::newDir(char* user) {
    return new qWorker::MySqlFsDirectory(_eDest, user);
}

XrdSfsFile* qWorker::MySqlFs::newFile(char* user) {
#ifdef NO_XROOTD_FS
    return new qWorker::MySqlFsFile(
                                    _eDest, user, 
                                    boost::make_shared<FakeAddCallback>(),
                                    boost::make_shared<FakeFileValidator>(),
                                    _service);
#else
    return new qWorker::MySqlFsFile(
                                    _eDest, user, 
                                    boost::make_shared<AddCallbackFunc>(),
                                    boost::make_shared<FileValidator>(_localroot),
                                    _service);
#endif
}

// Other Functions
//
int qWorker::MySqlFs::chmod(
    char const* Name, XrdSfsMode Mode, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::exists(
    char const* fileName, XrdSfsFileExistence& existsFlag,
    XrdOucErrInfo& outError, XrdSecEntity const* client,
    char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::fsctl(
    int const cmd, char const* args, XrdOucErrInfo& outError,
    XrdSecEntity const* client) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::getStats(char* buff, int blen) {
    return SFS_ERROR;
}

char const* qWorker::MySqlFs::getVersion(void) {
    return "$Id$";
}

int qWorker::MySqlFs::mkdir(
    char const* dirName, XrdSfsMode Mode, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::prepare(XrdSfsPrep& pargs, XrdOucErrInfo& outError,
                         XrdSecEntity const* client) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

/// rem() : discard/squash a query result and the running/queued query
///  that would-have/has-had produced it.
int qWorker::MySqlFs::rem(char const* path, XrdOucErrInfo& outError,
                          XrdSecEntity const* client, char const* opaque) {
    // Check for qserv result path
    fs::FileClass c = fs::computeFileClass(path);
    if(c != fs::TWO_READ) { // Only support removal of result files.
        outError.setErrInfo(ENOTSUP, "Operation not supported");
        return SFS_ERROR;
    }
    std::string hash = fs::stripPath(path);
    // Signal query squashing
    qWorker::QueryRunner::Manager& mgr = qWorker::QueryRunner::getMgr();
    mgr.squashByHash(hash);
    return SFS_OK;
}

int qWorker::MySqlFs::remdir(char const* dirName, XrdOucErrInfo& outError,
                             XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::rename(
    char const* oldFileName, char const* newFileName, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaqueO, char const* opaqueN) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::stat(
    char const* Name, struct stat* buf, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::stat(char const* Name, mode_t& mode, 
                           XrdOucErrInfo& outError,
                           XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

int qWorker::MySqlFs::truncate(
    char const* Name, XrdSfsFileOffset fileOffset, XrdOucErrInfo& outError,
    XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

class XrdSysLogger;

extern "C" {

XrdSfsFileSystem* XrdSfsGetFileSystem(
    XrdSfsFileSystem* native_fs, XrdSysLogger* lp, char const* fileName) {
    static XrdSysError eRoute(lp, "MySqlFs");
    static qWorker::MySqlFs myFS(&eRoute, fileName);

    eRoute.Say("MySqlFs (MySQL File System)");
    eRoute.Say(myFS.getVersion());
    return &myFS;
}

} // extern "C"
