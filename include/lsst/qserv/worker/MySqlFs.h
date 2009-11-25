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
};

}}} // namespace lsst::qserv::worker

#endif
