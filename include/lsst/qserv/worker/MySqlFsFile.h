#ifndef LSST_LSPEED_MYSQLFSFILE_H
#define LSST_LSPEED_MYSQLFSFILE_H

#include <sys/types.h>
    
#include <deque>
#include <string>
#include <sstream>
#ifdef DO_NOT_USE_BOOST
#include "XrdSys/XrdSysPthread.hh"
#else
#include "boost/thread.hpp"
#endif
#include "XrdSfs/XrdSfsInterface.hh"
#include "mysql/mysql.h"

class XrdSysError;
class XrdSysLogger;
class XrdSfsAio;

namespace lsst {
namespace qserv {
namespace worker {

class MySqlFsFile : public XrdSfsFile {
public:
    MySqlFsFile(XrdSysError* lp, char* user = 0);
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
    class StringBuffer {
    public:
        StringBuffer() : _totalSize(0) {}
	~StringBuffer() { reset(); }
	void addBuffer(XrdSfsFileOffset offset, char const* buffer, 
		       XrdSfsXferSize bufferSize);
	std::string getStr() const;
	XrdSfsFileOffset getLength() const;
	std::string getDigest() const;
	void reset();
    private:
	struct Fragment {
	    Fragment(XrdSfsFileOffset offset_, char const* buffer_, 
		     XrdSfsXferSize bufferSize_) 
	    : offset(offset_), buffer(buffer_), bufferSize(bufferSize_) {}

	    XrdSfsFileOffset offset; 
	    char const* buffer;
	    XrdSfsXferSize bufferSize;
	};

	typedef std::deque<Fragment> FragmentDeque;
	FragmentDeque _buffers;
	XrdSfsFileOffset _totalSize;
#if DO_NOT_USE_BOOST
	XrdSysMutex _mutex;
#else
	boost::mutex _mutex;
#endif
	std::stringstream _ss;
    };

    bool _addWritePacket(XrdSfsFileOffset offset, char const* buffer, 
			 XrdSfsXferSize bufferSize);
    bool _flushWrite();
    bool _hasPacketEof(char const* buffer, XrdSfsXferSize bufferSize) const;

    std::string _runScriptPiece(MYSQL*const db,
				std::string const& scriptId, 
				std::string const& pieceName,
				std::string const& piece);
    std::string _runScriptPieces(MYSQL*const db, 
				 std::string const& scriptId,
				 std::string const& build, 
				 std::string const& run, 
				 std::string const& cleanup);
    bool _runScript(std::string const& script, std::string const& dbName);
    void _setDumpNameAsChunkId();

    XrdSysError* _eDest;
    int _chunkId;
    std::string _userName;
    std::string _dumpName;
    std::string _socketFilename;
    std::string _mysqldumpPath;
    StringBuffer _queryBuffer;
};

}}} // namespace lsst::qserv::worker

#endif
