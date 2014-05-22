// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#include "xrdsvc/SsiSession.h"

// System headers
#include <iostream>
#include <string>

// Third-party
#include "XrdSsi/XrdSsiRequest.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/ProtoImporter.h"
#include "proto/worker.pb.h"
#include "util/Timer.h"
#include "wbase/MsgProcessor.h"
#include "wbase/SendChannel.h"
#include "wlog/WLogger.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

typedef proto::ProtoImporter<proto::TaskMsg> Importer;
typedef boost::shared_ptr<Importer> ImporterPtr;
////////////////////////////////////////////////////////////////////////
// class SsiSession::ReplyChannel
////////////////////////////////////////////////////////////////////////

class SsiSession::ReplyChannel : public wbase::SendChannel {
public:
    typedef XrdSsiResponder::Status Status;
    typedef boost::shared_ptr<ReplyChannel> Ptr;

    ReplyChannel(SsiSession& s) : ssiSession(s) {}

    virtual bool send(char const* buf, int bufLen) {
        Status s = ssiSession.SetResponse(buf, bufLen);
        if(s != XrdSsiResponder::wasPosted) {
            std::ostringstream os;
            os << "DANGER: Couldn't post response of length="
               << bufLen << std::endl;
            ssiSession._log->error(os.str());
            return false;
        }
        return true;
    }

    virtual bool sendError(std::string const& msg, int code) {
        Status s = ssiSession.SetErrResponse(msg.c_str(), code);
        if(s != XrdSsiResponder::wasPosted) {
            std::ostringstream os;
            os << "DANGER: Couldn't post error response " << msg
               << std::endl;
            ssiSession._log->error(os.str());
            return false;
        }
        return true;
    }
    virtual bool sendFile(int fd, Size fSize) {
        util::Timer t;
        t.start();
        Status s = ssiSession.SetResponse(fSize, fd);
        std::ostringstream os;
        if(s == XrdSsiResponder::wasPosted) {
            os << "file posted ok";
        } else {
            if(s == XrdSsiResponder::notActive) {
                os << "DANGER: Couldn't post response file of length="
                   << fSize << " responder not active.\n";
            } else {
                os << "DANGER: Couldn't post response file of length="
                   << fSize << std::endl;
            }
            release();
            sendError("Internal error posting response file", 1);
            return false; // sendError handles everything else.
        }
        ssiSession._log->error(os.str());
        t.stop();
        os.str("");
        os << "sendFile took " << t.getElapsed() << " seconds";
        ssiSession._log->info(os.str());
        return true;
    }
    SsiSession& ssiSession;
};
////////////////////////////////////////////////////////////////////////
// class SsiProcessor
////////////////////////////////////////////////////////////////////////

/// Feed ProtoImporter results to msgprocessor by bundling the responder as a
/// SendChannel
struct SsiProcessor : public Importer::Acceptor {
    typedef boost::shared_ptr<SsiProcessor> Ptr;
    SsiProcessor(ResourceUnit const& ru_,
                 wbase::MsgProcessor::Ptr mp,
                 boost::shared_ptr<wbase::SendChannel> sc)
        : ru(ru_), msgProcessor(mp), sendChannel(sc) {}

    virtual void operator()(boost::shared_ptr<proto::TaskMsg> m) {
        util::Timer t;
        if(m->has_db() && m->has_chunkid()
           && (ru.db() == m->db()) && (ru.chunk() == m->chunkid())) {
            t.start();
            (*msgProcessor)(m, sendChannel);
            t.stop();
            std::cerr << "SsiProcessor msgProcessor call took "
                      << t.getElapsed() << " seconds" << std::endl;
        } else {
            std::ostringstream os;
            os << "Mismatched db/chunk in msg on resource db="
               << ru.db() << " chunkId=" << ru.chunk();
            sendChannel->sendError(os.str().c_str(), EINVAL);
        }
    }
    ResourceUnit const& ru;
    wbase::MsgProcessor::Ptr msgProcessor;
    boost::shared_ptr<wbase::SendChannel> sendChannel;
};
////////////////////////////////////////////////////////////////////////
// class SsiSession
////////////////////////////////////////////////////////////////////////

// Step 4
bool
SsiSession::ProcessRequest(XrdSsiRequest* req, unsigned short timeout) {
    util::Timer t;
    // Figure out what the request is.
    std::ostringstream os;
    os << "ProcessRequest, service=" << sessName;
    _log->info(os.str());
    t.start();
    BindRequest(req, this); // Step 5
    t.stop();
    os.str("");
    os << "BindRequest took " << t.getElapsed() << " seconds";
    _log->info(os.str());

    char *reqData = 0;
    int   reqSize;
    t.start();
    reqData = req->GetRequest(reqSize);
    t.stop();
    os.str("");
    os << "GetRequest took " << t.getElapsed() << " seconds";
    _log->info(os.str());

    os.str("");
    os << "### " << reqSize <<" byte request: "
       << std::string(reqData, reqSize);
    _log->info(os.str());
    ResourceUnit ru(sessName);
    if(ru.unitType() == ResourceUnit::DBCHUNK) {
        if(!(*_validator)(ru)) {
            os.str("");
            os << "WARNING: unowned chunk query detected: "
               << ru.path();
            _log->warn(os.str());
            //error.setErrInfo(ENOENT, "File does not exist");
            return false;
        }

        t.start();
        enqueue(ru, reqData, reqSize);
        t.stop();
        os.str("");
        os << "SsiSession::enqueue took " << t.getElapsed() << " seconds";
        _log->info(os.str());

        ReleaseRequestBuffer();
    } else {
        // Ignore this request.
        // Should send an error...
        os.str("");
        os << "TODO: Should send an error for Garbage request:"
           << sessName << std::endl;
        _log->info(os.str());
        ReleaseRequestBuffer();
        return false;
    }
    return true;
}

void
SsiSession::RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo,
                            bool cancel) { // Step 8
    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)

    // No buffers allocated, so don't need to free.
    // We can release/unlink the file now
    std::ostringstream os;
    os << "RequestFinished ";
    switch(rinfo.rType) {
    case XrdSsiRespInfo::isNone: os << "type=isNone"; break;
    case XrdSsiRespInfo::isData: os << "type=isData"; break;
    case XrdSsiRespInfo::isError: os << "type=isError"; break;
    case XrdSsiRespInfo::isFile: os << "type=isFile"; break;
    case XrdSsiRespInfo::isStream: os << "type=isStream"; break;
    }
    // We can't do much other than close the file.
    // It should work (on linux) to unlink the file after we open it, though.
    _log->info(os.str());
}

bool
SsiSession::Unprovision(bool forced) {
    // all requests guaranteed to be finished or cancelled.
    delete this;
    return true; // false if we can't unprovision now.
}

void SsiSession::enqueue(ResourceUnit const& ru, char* reqData, int reqSize) {

    // reqData has the entire request, so we can unpack it without waiting for
    // more data.
    ReplyChannel::Ptr rc(new ReplyChannel(*this));
    SsiProcessor::Ptr sp(new SsiProcessor(ru, _processor, rc));
//    Importer::Acceptor imp(new Importer(sp));
    std::ostringstream os;
    os << "Importing TaskMsg of size " << reqSize;
    _log->info(os.str());
    proto::ProtoImporter<proto::TaskMsg> pi(sp);

    pi(reqData, reqSize);
    if(pi.numAccepted() < 1) {
        // TODO Report error.
    } else {
        os.str("");
        os << "enqueued task ok: " << ru;
        _log->error(os.str());

    }
}

}}} // lsst::qserv::xrdsvc
