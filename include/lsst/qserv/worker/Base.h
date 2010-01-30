#ifndef LSST_QSERV_WORKER_BASE_H
#define LSST_QSERV_WORKER_BASE_H

// Std
#include <deque>
#include <string>
// Xrootd
#include "XrdSfs/XrdSfsInterface.hh"

#ifdef DO_NOT_USE_BOOST
#include "XrdSys/XrdSysPthread.hh"
#else
#include "boost/thread.hpp"
#include "boost/format.hpp"
#endif

class XrdSysError;
class XrdSysLogger;
class XrdSfsAio;

namespace lsst {
namespace qserv {
namespace worker {
// Forward:
class StringBuffer;

// Constants
extern std::string DUMP_BASE;
extern std::string CREATE_SUBCHUNK_SCRIPT; 
extern std::string CLEANUP_SUBCHUNK_SCRIPT;


// Hashing-related
std::string hashQuery(char const* buffer, int bufferSize);
std::string hashToPath(std::string const& hash);
std::string hashToResultPath(std::string const& hash);

struct ScriptMeta {
    ScriptMeta(StringBuffer const& b, int chunkId_);
    std::string script;
    std::string hash;
    std::string dbName;
    std::string resultPath;
    int chunkId;
};

 
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

}}}

#if DO_NOT_USE_BOOST
typedef lsst::qserv::worker::PosFormat Pformat;
#else  
typedef boost::format Pformat;
#endif

#endif // LSST_QSERV_WORKER_BASE_H
