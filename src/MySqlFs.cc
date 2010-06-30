#include "lsst/qserv/worker/MySqlFs.h"

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSfs/XrdSfsCallBack.hh" // For Open-callbacks(FinishListener)

#include "lsst/qserv/worker/MySqlFsDirectory.h"
#include "lsst/qserv/worker/MySqlFsFile.h"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/Config.h"
#include <cerrno>
#include <iostream>

// Externally declare XrdSfs loader to cheat on Andy's suggestion.
extern XrdSfsFileSystem *XrdXrootdloadFileSystem(XrdSysError *, char *, 
						 const char *);
namespace qWorker = lsst::qserv::worker;

namespace { 
 
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
} // anonymous namespace

qWorker::MySqlFs::MySqlFs(XrdSysError* lp, char const* cFileName) 
  : XrdSfsFileSystem(), _eDest(lp) {
    static boost::mutex m;
    boost::lock_guard<boost::mutex> l(m);
    _eDest->Say("MySqlFs loading libXrdOfs.so for clustering cmsd support.");
    _isMysqlFail = mysql_library_init(0, NULL, NULL);
    if(_isMysqlFail) {
	_eDest->Say("Problem initializing MySQL library. Behavior undefined.");
    }
    if(!getConfig().getIsValid()) {
        _eDest->Say(("Configration invalid: " + getConfig().getError() 
                     + " -- Behavior undefined.").c_str());
    }
#ifdef NO_XROOTD_FS
#else
    XrdSfsFileSystem* fs;
    fs = XrdXrootdloadFileSystem(_eDest, "libXrdOfs.so", cFileName);
    if(fs == 0) {
	_eDest->Say("Problem loading libXrdOfs.so. Clustering won't work.");
    }
#endif
    updateResultPath();
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
    return new qWorker::MySqlFsFile(_eDest, user, 
				    boost::make_shared<AddCallbackFunc>());
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

int qWorker::MySqlFs::rem(char const* path, XrdOucErrInfo& outError,
                     XrdSecEntity const* client, char const* opaque) {
    outError.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
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

int qWorker::MySqlFs::stat(char const* Name, mode_t& mode, XrdOucErrInfo& outError,
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

