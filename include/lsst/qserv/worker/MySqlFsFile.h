#ifndef LSST_LSPEED_MYSQLFSFILE_H
#define LSST_LSPEED_MYSQLFSFILE_H

#include <sys/types.h>
    
#include <string>

#include "XrdSfs/XrdSfsInterface.hh"

class XrdSysError;
class XrdSysLogger;
class XrdSfsAio;

namespace lsst {
namespace qserv {
namespace worker {

class MySqlFsFile : public XrdSfsFile {
public:
    MySqlFsFile(char* user = 0);
    ~MySqlFsFile(void);

    int open(char const* fileName, XrdSfsFileOpenMode openMode,
             mode_t createMode, XrdSecEntity const* client = 0,
    char const* opaque = 0);
    int close(void);
    int fctl(int const cmd, char const* args, XrdOucErrInfo& outError);
    char const* FName(void);

    int getMmap(void** Addr, off_t& Size);

    int read(XrdSfsFileOffset fileOffset, XrdSfsXferSize prereadSz);
    XrdSfsXferSize read(XrdSfsFileOffset fileOffset, char* buffer,
                        XrdSfsXferSize bufferSize);
    int read(XrdSfsAio* aioparm);

    XrdSfsXferSize write(XrdSfsFileOffset fileOffset, char const* buffer,
                         XrdSfsXferSize bufferSize);
    int write(XrdSfsAio* aioparm);

    int sync(void);

    int sync(XrdSfsAio* aiop);

    int stat(struct stat* buf);

    int truncate(XrdSfsFileOffset fileOffset);

    int getCXinfo(char cxtype[4], int& cxrsz);

private:

    bool _runScript(std::string const& script, std::string const& dbName);

    int _chunkId;
    std::string _userName;
    std::string _dumpName;
};

}}} // namespace lsst::qserv::worker

#endif
