//#define FAKE_XRD 1

#ifdef FAKE_XRD
#include <iostream>
#else
#include "XrdPosix/XrdPosixExtern.hh"
#endif

#include "lsst/qserv/master/xrdfile.h"

namespace qMaster = lsst::qserv::master;

#ifdef FAKE_XRD // Fake placeholder implemenation
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

#else // Not faked: choose the real XrdPosix implementation.

int qMaster::xrdOpen(const char *path, int oflag) {
    return XrdPosix_Open(path, oflag);
}

long long qMaster::xrdRead(int fildes, void *buf, unsigned long long nbyte) {
    return XrdPosix_Read(fildes, buf, nbyte); 
}

long long qMaster::xrdWrite(int fildes, const void *buf, 
			    unsigned long long nbyte) {
    return XrdPosix_Write(fildes, buf, nbyte);
}

int qMaster::xrdClose(int fildes) {
    return XrdPosix_Close(fildes);
}
#endif

