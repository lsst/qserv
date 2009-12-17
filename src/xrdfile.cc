//#define FAKE_XRD 1
#include <iostream>
#ifdef FAKE_XRD

#else
#include "XrdPosix/XrdPosixExtern.hh"
#include "XrdClient/XrdClientConst.hh"
#include "XrdClient/XrdClientEnv.hh"
#include <limits>
#include <fcntl.h>
#endif

#include "lsst/qserv/master/xrdfile.h"

namespace qMaster = lsst::qserv::master;

// Statics
static bool qMasterXrdInitialized = false; // Library initialized?

#ifdef FAKE_XRD // Fake placeholder implemenation
void qMaster::xrdInit() {}

int qMaster::xrdOpen(const char *path, int oflag) {
    static int fakeDes=50;
    std::cout << "xrd openfile " << path << " returning ("
	      << fakeDes << ")" << std::endl;
    return fakeDes;
}

long long qMaster::xrdRead(int fildes, void *buf, unsigned long long nbyte) {
    static char fakeResults[] = "This is totally fake.";
    int len=strlen(fakeResults);
    std::cout << "xrd read " << fildes << ": faked" << std::endl;
    if(nbyte > static_cast<unsigned long long>(len)) {
	nbyte = len+1;
    }
    memcpy(buf, fakeResults, nbyte);
    return nbyte;
}

long long qMaster::xrdWrite(int fildes, const void *buf, 
			    unsigned long long nbyte) {
    std::string s;
    s.assign(static_cast<const char*>(buf), nbyte);
    std::cout << "xrd write (" <<  fildes << ") \"" 
	      << s << std::endl;
    return nbyte;
}

int qMaster::xrdClose(int fildes) {
    std::cout << "xrd close (" << fildes << ")" << std::endl;
    return 0; // Always pretend to succeed.
}

long long qMaster::xrdLseekSet(int fildes, off_t offset) {
    return offset; // Always pretend to succeed
}

#else // Not faked: choose the real XrdPosix implementation.

void qMaster::xrdInit() {
    qMasterXrdInitialized = true;

    // Set timeouts to effectively disable client timeouts.
    //EnvPutInt(NAME_CONNECTTIMEOUT, 3600*24*10); // Don't set this!
    
    EnvPutInt(NAME_REQUESTTIMEOUT, std::numeric_limits<int>::max());
    EnvPutInt(NAME_DATASERVERCONN_TTL, std::numeric_limits<int>::max());
    // Can't set to max, since it gets added to time(), and max would overflow.
    // Set to 3 years.
    EnvPutInt(NAME_TRANSACTIONTIMEOUT, 60*60*24*365*3); 

    // Don't need to lengthen load-balancer timeout.??
    //EnvPutInt(NAME_LBSERVERCONN_TTL, std::numeric_limits<int>::max());
}

int qMaster::xrdOpen(const char *path, int oflag) {
    if(!qMasterXrdInitialized) { xrdInit(); }
    time_t seconds;
    //time(&seconds);
    //std::cout << ::asctime(localtime(&seconds)) << "Open " << path << " in flight\n";
    int res = XrdPosix_Open(path, oflag);
    //time(&seconds);
    //std::cout << ::asctime(localtime(&seconds)) << "Open " << path << " finished.\n";
    return res;
}

long long qMaster::xrdRead(int fildes, void *buf, unsigned long long nbyte) {
    // std::cout << "xrd trying to read (" <<  fildes << ") " 
    // 	      << nbyte << " bytes" << std::endl;
    time_t seconds;
    //time(&seconds);
    //std::cout << ::asctime(localtime(&seconds)) << "Read " << fildes << " in flight\n";
    long long readCount;
    readCount = XrdPosix_Read(fildes, buf, nbyte); 
    //time(&seconds);
    //std::cout << ::asctime(localtime(&seconds)) << "Read " << fildes << " finished.\n";    //std::cout << "read " << readCount << " from xrd." << std::endl;
    return readCount;
}

long long qMaster::xrdWrite(int fildes, const void *buf, 
			    unsigned long long nbyte) {
    // std::string s;
    // s.assign(static_cast<const char*>(buf), nbyte);
    // std::cout << "xrd write (" <<  fildes << ") \"" 
    // 	      << s << "\"" << std::endl;
    time_t seconds;
    //time(&seconds);
    //std::cout << ::asctime(localtime(&seconds)) << "Write " << fildes << " in flight\n";
    long long res = XrdPosix_Write(fildes, buf, nbyte);
    //time(&seconds);
    //std::cout << ::asctime(localtime(&seconds)) << "Write " << fildes << " finished. (" << res << ")\n";
  return res;
}

int qMaster::xrdClose(int fildes) {
    return XrdPosix_Close(fildes);
}
 
long long qMaster::xrdLseekSet(int fildes, unsigned long long offset) {
    return XrdPosix_Lseek(fildes, offset, SEEK_SET);
}

int qMaster::xrdReadStr(int fildes, char *buf, int len) {
    return xrdRead(fildes, static_cast<void*>(buf), 
		   static_cast<unsigned long long>(len));
}

/// Return codes for writing and reading are written to *write and *read.
/// Both will be attempted as independently as possible.
/// e.g., if writing fails, the read will drain the file into nothingness.
/// If reading fails, writing can still succeed in writing as much as was read.
///
/// @param fildes -- XrdPosix file descriptor
/// @param fragmentSize -- size to grab from xrootd server 
///        (64K <= size <= 100MB; a few megs are good)
/// @param filename -- filename of file that will receive the result 
/// @return write -- How many bytes were written, or -errno (negative errno).
/// @return read -- How many bytes were read, or -errno (negative errno).
void qMaster::xrdReadToLocalFile(int fildes, int fragmentSize, 
				 const char* filename, 
				 int* write, int* read) {
    size_t bytesRead = 0;
    size_t bytesWritten = 0;
    int writeRes = 0;
    int readRes = 0;
    const int minFragment = 65536;// Prevent fragments smaller than 64K.
    void* buffer = NULL;

    if(fragmentSize < minFragment) fragmentSize = minFragment; 
    buffer = malloc(fragmentSize);

    if(buffer == NULL) {  // Bail out if we can't allocate.
	*write = -1; 
	*read = -1;
	return;
    }

    int localFileDesc = open(filename, O_CREAT|O_WRONLY|O_TRUNC);
    if(localFileDesc == -1) {
	writeRes = -errno;
    }
    while(1) {
	readRes = xrdRead(fildes, buffer, 
			      static_cast<unsigned long long>(fragmentSize));
	if(readRes <= 0) { // Done, or error.
	    readRes = -errno;
	    break;
	}
	if(writeRes >= 0) { // No error yet?
	    writeRes = pwrite(localFileDesc, buffer, 
			      readRes, bytesWritten);
	    if(writeRes != -1) {
		bytesWritten += writeRes;
	    } else {
		writeRes = -errno; // Update write status
	    }
	}
	if(readRes < fragmentSize) {
	    break;
	}
    }
    // Close the writing file.
    if(localFileDesc != -1) {
	int res = close(localFileDesc);
	if((res == -1) && (writeRes >= 0)) {
	    writeRes = -errno;
	} else {
	    writeRes = bytesWritten; // Update successful result.
	    if(readRes >= 0) {
		readRes = bytesRead;
	    }
	}
    }
    free(buffer);
    *write = writeRes;
    *read = readRes;
    return;
}

qMaster::XrdTransResult qMaster::xrdOpenWriteReadSaveClose(
    const char *path, 
    const char* buf, int len, // Query
    int fragmentSize,
    const char* outfile) {

    XrdTransResult r; 

    int fh = xrdOpen(path, O_RDWR);
    if(fh == -1) {
	r.open = -errno;
	return r;
    } else {
	r.open = fh;
    }

    int writeCount = xrdWrite(fh, buf, len);
    if(writeCount != len) {
	r.queryWrite = -errno;
    } else {
	r.queryWrite = writeCount;
	xrdReadToLocalFile(fh, fragmentSize, outfile, 
			   &(r.localWrite), &(r.read));
    }
    xrdClose(fh);
    return r;
}


#endif

