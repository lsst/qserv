// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2014 LSST Corporation.
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
// xrdfile.h -- Wrapper for xrootd client API functions

#include "xrdc/xrdfile.h"

// System headers
#include <cassert>
#include <iostream>
#include <string>

// LSST headers
#include "lsst/log/Log.h"


//#define FAKE_XRD 1
//#define QSM_PROFILE_XRD 1

// Boost
#include <boost/thread/thread.hpp>

#if !FAKE_XRD
// System headers
#include <assert.h>
#include <fcntl.h>
#include <limits>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
// Third-party headers
#include "XrdPosix/XrdPosixLinkage.hh"
#include "XrdPosix/XrdPosixExtern.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdClient/XrdClientConst.hh"
#include "XrdClient/XrdClientEnv.hh"

#endif

#if QSM_PROFILE_XRD
#include "util/Timer.h"
#endif

#if FAKE_XRD // Fake placeholder implemenation

namespace lsst {
namespace qserv {
namespace xrdc {

int xrdOpen(const char *path, int oflag) {
    static int fakeDes=50;
    LOGF_DEBUG("xrd openfile %1% returning (%2%)" % path % fakeDes);
    return fakeDes;
}

long long xrdRead(int fildes, void *buf, unsigned long long nbyte) {
    static char fakeResults[] = "This is totally fake.";
    int len=strlen(fakeResults);
    LOGF_DEBUG("xrd read %1%: faked" % fildes);
    if(nbyte > static_cast<unsigned long long>(len)) {
        nbyte = len+1;
    }
    memcpy(buf, fakeResults, nbyte);
    return nbyte;
}

long long xrdWrite(int fildes, const void *buf,
                   unsigned long long nbyte) {
    std::string s;
    s.assign(static_cast<const char*>(buf), nbyte);
    LOGF_DEBUG("xrd write (%1%) \"%2%\"" % fildes % s);
    return nbyte;
}

int xrdClose(int fildes) {
    LOGF_DEBUG("xrd close (%1%)" % fildes);
    return 0; // Always pretend to succeed.
}

long long xrdLseekSet(int fildes, off_t offset) {
    return offset; // Always pretend to succeed
}

}}} // namespace lsst::qserv::xrdc

#else // Not faked: choose the real XrdPosix implementation.

namespace {

    struct XrdInit {
        static const int OPEN_FILES = 1024*1024*1024; // ~1 billion open files
        XrdPosixXrootd posixXrd;
        XrdInit();
    };

    XrdInit::XrdInit() : posixXrd(-OPEN_FILES) { // Use non-OS file descriptors

        // Set timeouts to effectively disable client timeouts.

        // Don't set this!
        //EnvPutInt(NAME_CONNECTTIMEOUT, 3600*24*10);

        // Don't set these for two-file model?
        //EnvPutInt(NAME_REQUESTTIMEOUT, std::numeric_limits<int>::max());
        //EnvPutInt(NAME_DATASERVERCONN_TTL, std::numeric_limits<int>::max());

        // TRANSACTIONTIMEOUT needs to get extended since it limits how
        // long the client will wait for an open() callback response.
        // Can't set to max, since it gets added to time(), and max would overflow.
        // Set to 3 years.
        EnvPutInt(NAME_TRANSACTIONTIMEOUT, 60*60*24*365*3);

        // Disable XrdClient read caching.
        EnvPutInt(NAME_READCACHESIZE, 0);

        // Don't need to lengthen load-balancer timeout.??
        //EnvPutInt(NAME_LBSERVERCONN_TTL, std::numeric_limits<int>::max());
    }

    XrdInit xrdInit;

    void recordTrans(char const* path, char const* buf, int len) {
        static char traceFile[] = "/dev/shm/xrdTransaction.trace";
        std::string record;
        {
            std::stringstream ss;
            ss << "####" << path << "####" << std::string(buf, len) << "####\n";
            record = ss.str();
        }
        int fd = ::open(traceFile, O_CREAT|O_WRONLY|O_APPEND);
        if (fd != -1) {
            ::write(fd, record.data(), record.length());
            ::close(fd);
        }
    }
}


#define LOGHACK1 LOGF_INFO(" %1% %2% in flight" % name % extra);
#define LOGHACK2 LOGF_INFO(" %1% s) %2% %3% finished" % t.getElapsed() % name % extra);


#if QSM_PROFILE_XRD
#   define QSM_TIMESTART(name, extra) \
    Timer t; \
    t.start(); \
    Timer::write(LOG_STRM(Info), t.startTime); \
    LOGHACK1
#   define QSM_TIMESTOP(name, extra) \
    t.stop(); \
    Timer::write(LOG_STRM(Info), t.stopTime); \
    LOGHACK2
#else // Turn off xrd call timing
#   define QSM_TIMESTART(name, path)
#   define QSM_TIMESTOP(name, path)
#endif

namespace lsst {
namespace qserv {
namespace xrdc {

int xrdOpen(const char *path, int oflag) {
#ifdef DBG_TEST_OPEN_FAILURE_1
    /*************************************************************
     * TEST FAILURE MODE: Intermittent XRD Open for Read Failure
     * ***********************************************************/
    if (oflag == O_RDONLY) {
        int coinToss = rand()%5;
        if (coinToss == 0) {
            LOGF_WARN("YOU ARE UNLUCKY (coin=%1%), SABOTAGING XRD OPEN!!!!" % coinToss);
            return -1;
        } else {
            LOGF_WARN("YOU DODGED A BULLET (coin=%1%), NO SABOTAGE THIS TIME!!" % coinToss);
        }
    }
    /*************************************************/
#elif defined DBG_TEST_OPEN_FAILURE_2
    /*************************************************************
     * TEST FAILURE MODE: Delay before XRD Open for Read
     * (Provides time to manually kill worker process for testing
     * chunk-level failure recovery.)
     * ***********************************************************/
    if (oflag == O_RDONLY) {
        LOGF_WARN("SLEEPING FOR 10 SECONDS");
        boost::this_thread::sleep(boost::posix_time::seconds(10));
    }
    /*************************************************/
#endif
    char const* abbrev = path;
    while((abbrev[0] != '\0') && *abbrev++ != '/');
    QSM_TIMESTART("Open", abbrev);
    LOGF_DEBUG("CALLING XrdPosixXrootd::Open(path =%1%, oflag =%2%)" % path % oflag);
    int res = XrdPosixXrootd::Open(path,oflag);
    LOGF_DEBUG("XrdPosixXrootd::Open() returned %1%" % res);
    QSM_TIMESTOP("Open", abbrev);
    return res;
}

int xrdOpenAsync(const char* path, int oflag, XrdPosixCallBack *cbP) {
    char const* abbrev = path;
    while((abbrev[0] != '\0') && *abbrev++ != '/');
    while((abbrev[0] != '\0') && *abbrev++ != '/');
    while((abbrev[0] != '\0') && *abbrev++ != '/');
    QSM_TIMESTART("OpenAsy", abbrev);
    LOGF_DEBUG("CALLING XrdPosixXrootd::Open()");
    int res = XrdPosixXrootd::Open(path, oflag, 0, cbP);
    // not sure what to do with mode, so set to 0 right now.
    QSM_TIMESTOP("OpenAsy", abbrev);
    assert(res == -1);
    return -errno; // Return something that indicates "in progress"
}

long long xrdRead(int fildes, void *buf, unsigned long long nbyte) {
    LOGF_DEBUG("xrd trying to read (%1%) nbyte %2% bytes" % fildes % nbyte);
    QSM_TIMESTART("Read", fildes);
    long long readCount;
    LOGF_DEBUG("CALLING XrdPosixXrootd::Read()");
    readCount = XrdPosixXrootd::Read(fildes, buf, nbyte);
    QSM_TIMESTOP("Read", fildes);

#ifdef DBG_TEST_READ_FAILURE_1
    /*************************************************
     * TEST FAILURE MODE: Reading query result fails.
     * ***********************************************/
    LOGF_WARN("SABOTAGING XRD READ!!!!");
    readCount = -1;
    /*************************************************/
#endif

#ifdef DBG_TEST_READ_FAILURE_2
    /*************************************************
     * TEST FAILURE MODE: Fuzz testing - simulate incomplete results.
     * ***********************************************/
    LOGF_WARN("SABOTAGING XRD READ!!!!");
    LOGF_WARN("XrdPosixXrootd::Read() returned: %1%" % readCount);
    readCount = rand()%readCount;
    LOGF_WARN("Set readCount = %1%" % readCount);
    /*************************************************/
#endif

#ifdef DBG_TEST_READ_FAILURE_3
    /*************************************************
     * TEST FAILURE MODE: Fuzz testing - simulate corrupted byte.
     * ***********************************************/
    LOGF_WARN("SABOTAGING XRD READ!!!!");
    LOGF_WARN("XrdPosixXrootd::Read() returned: %1%" % readCount);
    int position = rand()%readCount;
    char value = (char)(rand()%256);
    *((char *)buf + position) = value;
    /*************************************************/
#endif

#ifdef DBG_TEST_READ_FAILURE_4
    /*************************************************
     * TEST FAILURE MODE: Intermittent Read Failure
     * ***********************************************/
    int coinToss = rand()%10;
    if (coinToss == 0) {
        LOGF_WARN("YOU ARE UNLUCKY, SABOTAGING XRD READ!!!!");
        readCount = -1;
    } else {
        LOGF_WARN("YOU DODGED A BULLET, NO SABOTAGE THIS TIME!!");
    }
    /*************************************************/
#endif

    if (readCount < 0) {
         if (errno == 0) errno = EREMOTEIO;
         return -1;
    }

    return readCount;
}

long long xrdWrite(int fildes, const void *buf,
                   unsigned long long nbyte) {
    std::string s;
    s.assign(static_cast<const char*>(buf), nbyte);
    LOGF_DEBUG("xrd write (%1%) ) \"%2%\"" % fildes % s);
    QSM_TIMESTART("Write", fildes);
    LOGF_DEBUG("CALLING XrdPosixXrootd::Write()");
    long long res = XrdPosixXrootd::Write(fildes, buf, nbyte);
    QSM_TIMESTOP("Write", fildes);

#ifdef DBG_TEST_WRITE_FAILURE_1
    /*************************************************
     * TEST FAILURE MODE: Writing query result fails.
     * ***********************************************/
    LOGF_WARN("SABOTAGING XRD WRITE!!!!");
    res = -1;
    /*************************************************/
#endif

    if (res < 0) {
         if (errno == 0) errno = EREMOTEIO;
         return -1;
    }

    return res;
}

int xrdClose(int fildes) {
    QSM_TIMESTART("Close", fildes);
    LOGF_DEBUG("CALLING XrdPosixXrootd::Close()");
    int result = XrdPosixXrootd::Close(fildes);
    QSM_TIMESTOP("Close", fildes);
    return result;
}

long long xrdLseekSet(int fildes, unsigned long long offset) {
    LOGF_DEBUG("CALLING XrdPosixXrootd::Lseek()");
    return XrdPosixXrootd::Lseek(fildes, offset, SEEK_SET);
}

int xrdReadStr(int fildes, char *buf, int len) {
    return xrdRead(fildes, static_cast<void*>(buf),
                   static_cast<unsigned long long>(len));
}

std::string xrdGetEndpoint(int fildes) {
    // Re: XrdPosixXrootd::endPoint()
    // "the max you will ever need is 264 bytes"
    const int maxSize=265;
    char buffer[maxSize];
    LOGF_DEBUG("CALLING XrdPosixXrootd::endPoint()");
    int port = XrdPosixXrootd::endPoint(fildes, buffer, maxSize);
    if(port > 0) { // valid port?
        return std::string(buffer);
    } else {
        return std::string();
    }
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
/// @param abortFlag -- Flag to check to see if we've been aborted.
/// @return write -- How many bytes were written, or -errno (negative errno).
/// @return read -- How many bytes were read, or -errno (negative errno).
void xrdReadToLocalFile(int fildes, int fragmentSize,
                        const char* filename,
                        bool const *abortFlag,
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

    int localFileDesc = open(filename,
                             O_CREAT|O_WRONLY|O_TRUNC,
                             S_IRUSR|S_IWUSR);
    if(localFileDesc == -1) {
        while(errno == -EMFILE) {
            LOGF_WARN("EMFILE while trying to write locally.");
            sleep(1);
            localFileDesc = open(filename,
                                 O_CREAT|O_WRONLY|O_TRUNC,
                                 S_IRUSR|S_IWUSR);
        }
        writeRes = -errno;
    }
    while(1) {
        if(abortFlag && (*abortFlag)) break;
        readRes = xrdRead(fildes, buffer,
                          static_cast<unsigned long long>(fragmentSize));
        if(readRes <= 0) { // Done, or error.
            readRes = -errno;
            break;
        }
        bytesRead += readRes;
        if(writeRes >= 0) { // No error yet?
            while(1) {
                writeRes = pwrite(localFileDesc, buffer,
                                  readRes, bytesWritten);
                if(writeRes != -1) {
                    bytesWritten += writeRes;
                } else {
                    if(errno == ENOSPC) {
                        sleep(5); // sleep for 5 seconds.
                        continue; // Try again.
                    }
                    writeRes = -errno; // Update write status
                }
                break;
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
            LOGF_ERROR("Bad local close for descriptor %1%" % localFileDesc);
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

XrdTransResult xrdOpenWriteReadSaveClose(
    const char *path,
    const char* buf, int len, // Query
    int fragmentSize,
    const char* outfile) {

    XrdTransResult r;

    recordTrans(path, buf, len); // Record the trace file.

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
        xrdLseekSet(fh, 0);
        xrdReadToLocalFile(fh, fragmentSize, outfile, 0,
                           &(r.localWrite), &(r.read));
    }
    xrdClose(fh);
    return r;
}

XrdTransResult xrdOpenWriteReadSave(
    const char *path,
    const char* buf, int len, // Query
    int fragmentSize,
    const char* outfile) {

    XrdTransResult r;

    recordTrans(path, buf, len); // Record the trace file.

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
        xrdLseekSet(fh, 0);
        xrdReadToLocalFile(fh, fragmentSize, outfile, 0,
                           &(r.localWrite), &(r.read));
    }
    return r;
}

}}} // namespace lsst::qserv::xrdc

#endif

