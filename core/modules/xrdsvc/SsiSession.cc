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
#include <cctype>

// Third-party
#include "XrdSsi/XrdSsiRequest.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "lsst/log/Log.h"
#include "proto/ProtoImporter.h"
#include "proto/worker.pb.h"
#include "util/Timer.h"
#include "wbase/MsgProcessor.h"
#include "wbase/SendChannel.h"
#include "xrdsvc/SsiSession_ReplyChannel.h"

namespace {

char hexdigit(unsigned x) {
    if (x > 15) return '?';
    if (x > 9) return char(x - 10 + 'a');
    return char(x + '0');
}

// format buffer and replace non-printable characters with hex notation
std::string
quote(const char* data, int size) {
    std::string res;
    res.reserve(size);
    for (int i = 0; i != size; ++i) {
        char ch = data[i];
        if (std::iscntrl(ch)) {
            char buf[5] = "\\x00";
            buf[2] = ::hexdigit((ch >> 4) & 0xf);
            buf[3] = ::hexdigit(ch & 0xf);
            res += buf;
        } else {
            res += ch;
        }
    }
    return res;
}

}


namespace lsst {
namespace qserv {
namespace xrdsvc {

typedef proto::ProtoImporter<proto::TaskMsg> Importer;
typedef std::shared_ptr<Importer> ImporterPtr;

////////////////////////////////////////////////////////////////////////
// class SsiProcessor
////////////////////////////////////////////////////////////////////////

/// Feed ProtoImporter results to msgprocessor by bundling the responder as a
/// SendChannel
class SsiProcessor : public Importer::Acceptor {
public:
    typedef std::shared_ptr<SsiProcessor> Ptr;

    SsiProcessor(ResourceUnit const& ru,
                 wbase::MsgProcessor::Ptr mp,
                 std::shared_ptr<wbase::SendChannel> sc,
                 SsiSession& ssiSession)
        : _ru(ru),
          _msgProcessor(mp),
          _sendChannel(sc),
          _ssiSession(ssiSession) {}

    /// Accept a TaskMsg. Pass the msg to _msgProcessor, which returns a
    /// cancellation function to be called if the _msgProcessor's async
    /// operations (outputting through _sendChannel) should be cancelled
    virtual void operator()(std::shared_ptr<proto::TaskMsg> m) {
        util::Timer t;
        if(m->has_db() && m->has_chunkid()
           && (_ru.db() == m->db()) && (_ru.chunk() == m->chunkid())) {
            t.start();
            SsiSession::CancelFuncPtr p = (*_msgProcessor)(m, _sendChannel);
            _ssiSession._addCanceller(p);
            t.stop();
            LOGF_INFO("SsiProcessor msgProcessor call took %1% seconds" % t.getElapsed());
        } else {
            std::ostringstream os;
            os << "Mismatched db/chunk in msg on resource db="
               << _ru.db() << " chunkId=" << _ru.chunk();
            _sendChannel->sendError(os.str().c_str(), EINVAL);
        }
    }
private:
    ResourceUnit const& _ru;
    wbase::MsgProcessor::Ptr _msgProcessor;
    std::shared_ptr<wbase::SendChannel> _sendChannel;
    SsiSession& _ssiSession;
};
////////////////////////////////////////////////////////////////////////
// class SsiSession
////////////////////////////////////////////////////////////////////////

// Step 4
/// Called by XrdSsi to actually process a request.
bool
SsiSession::ProcessRequest(XrdSsiRequest* req, unsigned short timeout) {
    util::Timer t;
    // Figure out what the request is.
    LOGF_INFO("ProcessRequest, service=%1%" % sessName);
    t.start();
    BindRequest(req, this); // Step 5
    t.stop();
    LOGF_INFO("BindRequest took %1% seconds" % t.getElapsed());

    char *reqData = 0;
    int   reqSize;
    t.start();
    reqData = req->GetRequest(reqSize);
    t.stop();
    LOGF_INFO("GetRequest took %1% seconds" % t.getElapsed());

    LOGF_INFO("### %1% byte request: %2%" % reqSize % ::quote(reqData, reqSize));
    ResourceUnit ru(sessName);
    if(ru.unitType() == ResourceUnit::DBCHUNK) {
        if(!(*_validator)(ru)) {
            LOGF_WARN("WARNING: unowned chunk query detected: %1%" % ru.path());
            //error.setErrInfo(ENOENT, "File does not exist");
            return false;
        }

        t.start();
        _enqueue(ru, reqData, reqSize);
        t.stop();
        LOGF_INFO("SsiSession::enqueue took %1% seconds" % t.getElapsed());

        ReleaseRequestBuffer();
    } else {
        // Ignore this request.
        // Should send an error...
        LOGF_INFO("TODO: Should send an error for Garbage request: %1%" %
                sessName);
        ReleaseRequestBuffer();
        return false;
    }
    return true;
}

/// Called by XrdSsi to free resources.
void
SsiSession::RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo,
                            bool cancel) { // Step 8
    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)
    {
        boost::lock_guard<boost::mutex> lock(_cancelMutex);
        if(!_cancelled && cancel) { // Cancel if not already cancelled
            _cancelled = true;
            typedef std::vector<CancelFuncPtr>::iterator Iter;
            for(Iter i=_cancellers.begin(), e=_cancellers.end(); i != e; ++i) {
                assert(*i);
                (**i)();
            }
        }
    }
    // No buffers allocated, so don't need to free.
    // We can release/unlink the file now
    const char* type = "";
    switch(rinfo.rType) {
    case XrdSsiRespInfo::isNone: type = "type=isNone"; break;
    case XrdSsiRespInfo::isData: type = "type=isData"; break;
    case XrdSsiRespInfo::isError: type = "type=isError"; break;
    case XrdSsiRespInfo::isFile: type = "type=isFile"; break;
    case XrdSsiRespInfo::isStream: type = "type=isStream"; break;
    }
    // We can't do much other than close the file.
    // It should work (on linux) to unlink the file after we open it, though.
    LOGF_INFO("RequestFinished %1%" % type);
}

bool
SsiSession::Unprovision(bool forced) {
    // all requests guaranteed to be finished or cancelled.
    delete this;
    return true; // false if we can't unprovision now.
}

void SsiSession::_addCanceller(CancelFuncPtr p) {
    bool shouldCall = false;
    {
        boost::lock_guard<boost::mutex> lock(_cancelMutex);
        if(_cancelled) {
            // Don't add the canceller, just call it.
            shouldCall = true;
        } else {
            _cancellers.push_back(p);
        }
    }
    if (shouldCall) {
        (*p)(); // call outside of the lock
    }
}

// Accept an incoming request addressed to a ResourceUnit, with the particulars
/// defined in the reqData payload
void SsiSession::_enqueue(ResourceUnit const& ru, char* reqData, int reqSize) {

    // reqData has the entire request, so we can unpack it without waiting for
    // more data.
    ReplyChannel::Ptr rc(new ReplyChannel(*this));
    SsiProcessor::Ptr sp(new SsiProcessor(ru, _processor, rc, *this));

    LOGF_INFO("Importing TaskMsg of size %1%" % reqSize);
    proto::ProtoImporter<proto::TaskMsg> pi(sp);

    pi(reqData, reqSize);
    if(pi.getNumAccepted() < 1) {
        // TODO Report error.
    } else {
        LOGF_INFO("enqueued task ok: %1%" % ru);
    }
}

}}} // lsst::qserv::xrdsvc
