//#define FAKE_XRD 1
#include <iostream>
#ifdef FAKE_XRD

#else
#include "XrdPosix/XrdPosixExtern.hh"
#include "XrdClient/XrdClientConst.hh"
#include "XrdClient/XrdClientEnv.hh"
#include <limits>
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
    time(&seconds);
    std::cout << ::asctime(localtime(&seconds)) << "Open " << path << " in flight\n";
    int res = XrdPosix_Open(path, oflag);
    time(&seconds);
    std::cout << ::asctime(localtime(&seconds)) << "Open " << path << " finished.\n";
    return res;
}

long long qMaster::xrdRead(int fildes, void *buf, unsigned long long nbyte) {
    // std::cout << "xrd trying to read (" <<  fildes << ") " 
    // 	      << nbyte << " bytes" << std::endl;
    time_t seconds;
    time(&seconds);
    std::cout << ::asctime(localtime(&seconds)) << "Read " << fildes << " in flight\n";
    long long readCount;
    readCount = XrdPosix_Read(fildes, buf, nbyte); 
    time(&seconds);
    std::cout << ::asctime(localtime(&seconds)) << "Read " << fildes << " finished.\n";    //std::cout << "read " << readCount << " from xrd." << std::endl;
    return readCount;
}

int qMaster::xrdReadStr(int fildes, char *buf, int len) {
    return xrdRead(fildes, static_cast<void*>(buf), 
		   static_cast<unsigned long long>(len));
}

long long qMaster::xrdWrite(int fildes, const void *buf, 
			    unsigned long long nbyte) {
    // std::string s;
    // s.assign(static_cast<const char*>(buf), nbyte);
    // std::cout << "xrd write (" <<  fildes << ") \"" 
    // 	      << s << "\"" << std::endl;
    time_t seconds;
    time(&seconds);
    std::cout << ::asctime(localtime(&seconds)) << "Write " << fildes << " in flight\n";
    long long res = XrdPosix_Write(fildes, buf, nbyte);
    time(&seconds);
    std::cout << ::asctime(localtime(&seconds)) << "Write " << fildes << " finished. (" << res << ")\n";
  return res;
}

int qMaster::xrdClose(int fildes) {
    return XrdPosix_Close(fildes);
}
 
long long qMaster::xrdLseekSet(int fildes, unsigned long long offset) {
    return XrdPosix_Lseek(fildes, offset, SEEK_SET);
}

#endif

