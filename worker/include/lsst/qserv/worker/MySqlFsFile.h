// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2008-2013 LSST Corporation.
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
 
#ifndef LSST_LSPEED_MYSQLFSFILE_H
#define LSST_LSPEED_MYSQLFSFILE_H

#include <sys/types.h>
    
#include <string>
#include <sstream>
#include "XrdSfs/XrdSfsInterface.hh"
#include "mysql/mysql.h"

// pkg includes
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/ResultTracker.h"
#include "lsst/qserv/worker/MySqlFsCommon.h"
#include "lsst/qserv/worker/Service.h"

// common
#include "lsst/qserv/QservPath.hh"

// Forward
class XrdSysError;
class XrdSysLogger;
class XrdSfsAio;

namespace lsst {
namespace qserv {
namespace worker {
class RequestTaker; // Forward
class Logger;

/// A factory functor that exists for the MySqlFsFile to register a callback for
/// a completed query operation. The callback object is constructed with a
/// reference to the File object and attached to the ResultTracker. When the
/// ResultTracker receives the event (identified by filename), it fires the
/// callback, which triggers the calling File object. The functor abstraction
/// allows the MySqlFsFile object to avoid another direct dependence on xrootd
/// logic, enhancing testability outside of an xrootd running process. 
class AddCallbackFunction {
public:
    typedef boost::shared_ptr<AddCallbackFunction> Ptr;
    virtual ~AddCallbackFunction() {}
    virtual void operator()(XrdSfsFile& caller, std::string const& filename) = 0;
};

/// A file object used by xrootd to represent a single (open-)file context. 
/// Xrootd expects the object to support read/write and track its own position
/// in the "file".
class MySqlFsFile : public XrdSfsFile {
public:
    MySqlFsFile(boost::shared_ptr<Logger> log, char const* user = 0, 
                AddCallbackFunction::Ptr acf = AddCallbackFunction::Ptr(),
                fs::FileValidator::Ptr fv = fs::FileValidator::Ptr(),
                boost::shared_ptr<Service> service = Service::Ptr());
    virtual ~MySqlFsFile(void);

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
    int _acceptFile(char const* fileName); // New path handling code
    bool _addWritePacket(XrdSfsFileOffset offset, char const* buffer, 
                         XrdSfsXferSize bufferSize);
    void _addCallback(std::string const& filename);
    bool _flushWrite();
    bool _flushWriteDetach();
    bool _flushWriteSync();
    bool _hasPacketEof(char const* buffer, XrdSfsXferSize bufferSize) const;

    int _handleTwoReadOpen(char const* fileName);
    int _checkForHash(std::string const& hash);

    ResultErrorPtr _getResultState(std::string const& physFilename);

    void _setDumpNameAsChunkId();

    boost::shared_ptr<Logger> _log;
    AddCallbackFunction::Ptr _addCallbackF;
    fs::FileValidator::Ptr _validator;
    int _chunkId;
    std::string _userName;
    std::string _dumpName;
    bool _hasRead;
    StringBuffer2 _queryBuffer;
    boost::shared_ptr<QservPath> _path;
    boost::shared_ptr<RequestTaker> _requestTaker;
    boost::shared_ptr<Service> _service;

};

}}} // namespace lsst::qserv::worker

#endif
