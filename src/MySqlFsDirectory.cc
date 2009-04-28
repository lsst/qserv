#include "lsst/qserv/worker/MySqlFsDirectory.h"

#include "XrdSys/XrdSysError.hh"

#include <errno.h>

namespace qWorker = lsst::qserv::worker;

qWorker::MySqlFsDirectory::MySqlFsDirectory(XrdSysError* lp, char* user) :
    XrdSfsDirectory(user), _eDest(lp) {
}

qWorker::MySqlFsDirectory::~MySqlFsDirectory(void) {
}

int qWorker::MySqlFsDirectory::open(
    char const* dirName, XrdSecEntity const* client,
    char const* opaque) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

char const* qWorker::MySqlFsDirectory::nextEntry(void) {
    return 0;
}

int qWorker::MySqlFsDirectory::close(void) {
    error.setErrInfo(ENOTSUP, "Operation not supported");
    return SFS_ERROR;
}

char const* qWorker::MySqlFsDirectory::FName(void) {
    _eDest->Say("In MySqlFsDirectory::Fname()");
    return 0;
}
