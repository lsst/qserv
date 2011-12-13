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
 
#ifndef LSST_LSPEED_MYSQLFS_H
#define LSST_LSPEED_MYSQLFS_H

#include "XrdSfs/XrdSfsInterface.hh"

class XrdSysError;

namespace lsst {
namespace qserv {
namespace worker {

class MySqlFs : public XrdSfsFileSystem {
public:
    MySqlFs(XrdSysError* lp, char const* cFileName);
    virtual ~MySqlFs(void);

// Object Allocation Functions
//
    XrdSfsDirectory* newDir(char* user = 0);
    XrdSfsFile* newFile(char* user = 0);

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
    XrdSysError* _eDest;
    int _isMysqlFail;
    char const* _localroot;
};

}}} // namespace lsst::qserv::worker

extern "C" { 
// Forward
class XrdSfsFileSystem;
class XrdSysLogger;
                                      
XrdSfsFileSystem* XrdSfsGetFileSystem(XrdSfsFileSystem* native_fs, 
                                      XrdSysLogger* lp, 
                                      char const* fileName);
}
#endif
