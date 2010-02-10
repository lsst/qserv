#ifndef LSST_LSPEED_MYSQLFSFILE_H
#define LSST_LSPEED_MYSQLFSFILE_H

#include <sys/types.h>
    
#include <string>
#include <sstream>
#include "XrdSfs/XrdSfsInterface.hh"
#include "mysql/mysql.h"

// pkg includes
#include "lsst/qserv/worker/Base.h"

// Forward
class XrdSysError;
class XrdSysLogger;
class XrdSfsAio;

namespace lsst {
namespace qserv {
namespace worker {

class AddCallbackFunction {
public:
    typedef boost::shared_ptr<AddCallbackFunction> Ptr;
    virtual ~AddCallbackFunction() {}
    virtual void operator()(XrdSfsFile& caller, std::string const& filename) = 0;
};
    
class MySqlFsFile : public XrdSfsFile {
public:
    MySqlFsFile(XrdSysError* lp, char* user = 0, 
		AddCallbackFunction::Ptr acf = AddCallbackFunction::Ptr() );
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
    enum FileClass {COMBO, TWO_WRITE, TWO_READ, UNKNOWN};

    bool _addWritePacket(XrdSfsFileOffset offset, char const* buffer, 
			 XrdSfsXferSize bufferSize);
    void _addCallback(std::string const& filename);
    bool _flushWrite();
    bool _flushWriteDetach();
    bool _flushWriteSync();
    bool _hasPacketEof(char const* buffer, XrdSfsXferSize bufferSize) const;

    // File path functionality
    FileClass _getFileClass(std::string const& filename);
    std::string _stripPath(std::string const& filename);

    bool _isResultReady(std::string const& physFilename);
    //    bool _runScript(std::string const& script, std::string const& dbName);
    void _setDumpNameAsChunkId();

    XrdSysError* _eDest;
    AddCallbackFunction::Ptr _addCallbackF;
    int _chunkId;
    FileClass _fileClass;
    std::string _userName;
    std::string _dumpName;
    bool _hasRead;
    std::string _script;
    StringBuffer _queryBuffer;

};

}}} // namespace lsst::qserv::worker

#endif
