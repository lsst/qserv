// -*- LSST-C++ -*-
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

#ifndef LSST_QSERV_XRDFS_MYSQLFS_H
#define LSST_QSERV_XRDFS_MYSQLFS_H

// System headers
#include <set>

// Third-party headers
#include <boost/shared_ptr.hpp>
#include "XrdSfs/XrdSfsInterface.hh"

// Local headers
#include "lsst/log/Log.h"

// Forward declarations
class XrdSysError;
class XrdSysLogger;
namespace lsst {
namespace qserv {
namespace wcontrol {
    class Service;
}
namespace wpublish {
    class ChunkInventory;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace xrdfs {

/// MySqlFs is an xrootd fs plugin class
class MySqlFs : public XrdSfsFileSystem {
public:
    typedef std::set<std::string> StringSet;

    MySqlFs(XrdSysLogger* lp, char const* cFileName);
    virtual ~MySqlFs(void);

// Object Allocation Functions
//
// No idea what MonID is for (undocumented in xrootd)
    XrdSfsDirectory* newDir(char* user = 0, int MonID=0);
    XrdSfsFile* newFile(char* user = 0, int MonID=0);

// Other Functions
//
    int chmod(char const* Name, XrdSfsMode Mode, XrdOucErrInfo& outError,
              XrdSecEntity const* client = 0, char const* opaque = 0);

    int exists(char const* fileName, XrdSfsFileExistence& existsFlag,
               XrdOucErrInfo& outError, XrdSecEntity const* client = 0,
               char const* opaque = 0);

    int fsctl(int const cmd, char const* args, XrdOucErrInfo& outError,
              XrdSecEntity const* client = 0);

    int getStats(char* buff, int blen);

    char const* getVersion(void);

    int mkdir(char const* dirName, XrdSfsMode Mode, XrdOucErrInfo& outError,
              XrdSecEntity const* client = 0, char const* opaque = 0);

    int prepare(XrdSfsPrep& pargs, XrdOucErrInfo& outError,
                XrdSecEntity const* client = 0);

    int rem(char const* path, XrdOucErrInfo& outError,
            XrdSecEntity const* client = 0, char const* opaque = 0);

    int remdir(char const* dirName, XrdOucErrInfo& outError,
               XrdSecEntity const* client = 0, char const* opaque = 0);

    int rename(char const* oldFileName, char const* newFileName,
               XrdOucErrInfo& outError, XrdSecEntity const* client = 0,
               char const* opaqueO = 0, char const* opaqueN = 0);

    int stat(char const* Name, struct stat* buf, XrdOucErrInfo& outError,
             XrdSecEntity const* client = 0, char const* opaque = 0);

    int stat(char const* Name, mode_t& mode, XrdOucErrInfo& outError,
             XrdSecEntity const* client = 0, char const* opaque = 0);

    int truncate(char const* Name, XrdSfsFileOffset fileOffset,
                 XrdOucErrInfo& outError, XrdSecEntity const* client = 0,
                 char const* opaque = 0);
private:
    void _initExports();
    bool _cleanup();

    XrdSysError* _eDest;
    int _isMysqlFail;
    char const* _localroot;
    boost::shared_ptr<wcontrol::Service> _service;
    boost::shared_ptr<wpublish::ChunkInventory> _chunkInventory;
    LOG_LOGGER _log;
};

}}} // namespace lsst::qserv::xrdfs

extern "C" {
// Forward
class XrdSfsFileSystem;
class XrdSysLogger;

XrdSfsFileSystem* XrdSfsGetFileSystem(XrdSfsFileSystem* native_fs,
                                      XrdSysLogger* lp,
                                      char const* fileName);
}

#endif // LSST_QSERV_XRDFS_MYSQLFS_H
