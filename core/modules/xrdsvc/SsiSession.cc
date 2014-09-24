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
#include <deque>
#include <iostream>
#include <string>

// Third-party
#include <boost/thread/locks.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include "XrdSsi/XrdSsiRequest.hh"

// Qserv headers
#include "global/debugUtil.h"
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

/// ChannelStream is an implementation of an XrdSsiStream that accepts
/// SendChannel streamed data.
class ChannelStream : public XrdSsiStream {
public:
    /// SimpleBuffer is a really simple buffer for transferring data packets to
    /// XrdSsi
    class SimpleBuffer : public XrdSsiStream::Buffer {
    public:
        SimpleBuffer(std::string const& input) {
            data = new char[input.size()];
            memcpy(data, input.data(), input.size());
            next = 0;
        }

        //!> Call to recycle the buffer when finished
        virtual void Recycle() {
            delete this; // Self-destruct. FIXME: Not sure this is right.
        }

        // Inherited from XrdSsiStream:
        // char  *data; //!> -> Buffer containing the data
        // Buffer *next; //!> For chaining by buffer receiver

        virtual ~SimpleBuffer() {
            if(data) {
                delete[] data;
            }
        }
    };
    /// Constructor
    ChannelStream(wlog::WLogger::Ptr log)
        : XrdSsiStream(isActive),
          _closed(false),
          _log(log) {}
    /// Destructor
    virtual ~ChannelStream() {
        std::ostringstream os;
        os << "Stream (" << (void*)this << ") deleted";
        _log->info(os.str());
    }
    /// Push in a data packet
    void append(char const* buf, int bufLen, bool last) {
        if(_closed) {
            throw std::runtime_error("Stream closed, append(...,last=true) already received");
        }
        _log->info(" trying to append message");
        _log->info(makeByteStreamAnnotated("StreamMsg", buf, bufLen));
        {
            boost::unique_lock<boost::mutex> lock(_mutex);
            _log->info(" trying to append message (flowing)");

            _msgs.push_back(std::string(buf, bufLen));
            _closed = last; // if last is true, then we are closed.
            _hasDataCondition.notify_one();
        }
    }
    /// Pull out a data packet as a Buffer object (called by XrdSsi code)
    virtual Buffer *GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) {
        boost::unique_lock<boost::mutex> lock(_mutex);
        while(_msgs.empty() && !_closed) { // No msgs, but we aren't done
            // wait.
            _log->info("Waiting, no data ready");
            _hasDataCondition.wait(lock);
        }
        if(_msgs.empty() && _closed) { // We are closed and no more
            // msgs are available.
            _log->info("Not waiting, but closed");
            dlen = 0;
            eInfo.Set("Not an active stream", EOPNOTSUPP);
            return 0;
        }
        SimpleBuffer* sb = new SimpleBuffer(_msgs.front());
        dlen = _msgs.front().size();
        _msgs.pop_front();
        last = _closed && _msgs.empty();
        std::ostringstream os;
        os << "returning buffer (" << dlen << ","
           << (last ? "(more)" : "(last)");
        _log->info(os.str());
        return sb;
    }
    bool closed() const { return _closed; }
private:
    bool _closed;
    wlog::WLogger::Ptr _log;
    // Can keep a deque of (buf, bufsize) to reduce copying, if needed.
    std::deque<std::string> _msgs;
    boost::mutex _mutex;
    boost::condition_variable _hasDataCondition;
};

////////////////////////////////////////////////////////////////////////
// class SsiSession::ReplyChannel
////////////////////////////////////////////////////////////////////////
/// ReplyChannel is a SendChannel implementation that adapts XrdSsiSession
/// objects as backend data acceptors.
class SsiSession::ReplyChannel : public wbase::SendChannel {
public:
    typedef XrdSsiResponder::Status Status;
    typedef boost::shared_ptr<ReplyChannel> Ptr;

    ReplyChannel(SsiSession& s)
        : ssiSession(s), _stream(0) {}

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

    void _initStream() {
        //_stream.reset(new Stream);
        _stream = new ChannelStream(ssiSession._log);
        ssiSession.SetResponse(_stream);
    }

    virtual bool sendStream(char const* buf, int bufLen, bool last) {
        // Initialize streaming object if not initialized.
        std::ostringstream os;
        os << "sendStream, checking stream " << (void*) _stream << ")";
        ssiSession._log->info(os.str());
        if(!_stream) {
            _initStream();
        } else if(_stream->closed()) {
            return false;
        }
        _stream->append(buf, bufLen, last);
        return true;
    }
    SsiSession& ssiSession;
    ChannelStream* _stream;
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
                 boost::shared_ptr<wbase::SendChannel> sc,
                 std::vector<SsiSession::CancelFuncPtr>& cancellers_)
        : ru(ru_),
          msgProcessor(mp),
          sendChannel(sc),
          cancellers(cancellers_) {}

    virtual void operator()(boost::shared_ptr<proto::TaskMsg> m) {
        util::Timer t;
        if(m->has_db() && m->has_chunkid()
           && (ru.db() == m->db()) && (ru.chunk() == m->chunkid())) {
            t.start();
            SsiSession::CancelFuncPtr p = (*msgProcessor)(m, sendChannel);
            cancellers.push_back(p);
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
    std::vector<SsiSession::CancelFuncPtr>& cancellers;
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

/// Called by XrdSsi to free resources.
void
SsiSession::RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo,
                            bool cancel) { // Step 8
    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)

    if(cancel) {
        // Do cancellation.
        typedef std::vector<CancelFuncPtr>::iterator Iter;
        for(Iter i=_cancellers.begin(), e=_cancellers.end(); i != e; ++i) {
            assert(*i);
            (**i)();
        }
    }
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

/// Accept an incoming request addressed to a ResourceUnit, with the particulars
/// defined in the reqData payload
void SsiSession::enqueue(ResourceUnit const& ru, char* reqData, int reqSize) {

    // reqData has the entire request, so we can unpack it without waiting for
    // more data.
    ReplyChannel::Ptr rc(new ReplyChannel(*this));
    SsiProcessor::Ptr sp(new SsiProcessor(ru, _processor, rc, _cancellers));
    std::ostringstream os;
    os << "Importing TaskMsg of size " << reqSize;
    _log->info(os.str());
    proto::ProtoImporter<proto::TaskMsg> pi(sp);

    pi(reqData, reqSize);
    if(pi.getNumAccepted() < 1) {
        // TODO Report error.
    } else {
        os.str("");
        os << "enqueued task ok: " << ru;
        _log->error(os.str());
    }
}

}}} // lsst::qserv::xrdsvc
