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
#ifndef LSST_QSERV_CCONTROL_MERGINGREQUESTER_H
#define LSST_QSERV_CCONTROL_MERGINGREQUESTER_H

// Third-party headers
#include <memory>
#include "boost/thread/mutex.hpp"

// Qserv headers
#include "qdisp/ResponseRequester.h"

// Forward decl
namespace lsst {
namespace qserv {
  class MsgReceiver;
namespace proto {
  class WorkerResponse;
}
namespace rproc {
  class InfileMerger;
}}}

namespace lsst {
namespace qserv {
namespace ccontrol {

/// MergingRequester is an implementation of a ResponseRequester that implements
/// czar-side knowledge of the worker's response protocol. It leverages XrdSsi's
/// API by pulling the exact number of bytes needed for the next logical
/// fragment instead of performing buffer size and offset
/// management. Fully-constructed protocol messages are then passed towards an
/// InfileMerger.
class MergingRequester : public qdisp::ResponseRequester {
public:
    /// Possible MergingRequester message state
    enum class MsgState { INVALID, HEADER_SIZE_WAIT,
                    RESULT_WAIT, RESULT_EXTRA,
                    RESULT_RECV, BUFFER_DRAIN,
                    HEADER_ERR, RESULT_ERR };
    static const char* getStateStr(MsgState const& st);

    typedef std::shared_ptr<MergingRequester> Ptr;
    virtual ~MergingRequester() {}

    /// @param msgReceiver Message code receiver
    /// @param merger downstream merge acceptor
    /// @param tableName target table for incoming data
    MergingRequester(std::shared_ptr<MsgReceiver> msgReceiver,
                     std::shared_ptr<rproc::InfileMerger> merger,
                     std::string const& tableName);

    /// @return a char vector to receive the next message. The vector
    /// should be sized to the request size. The buffer will be filled
    /// before flush(), unless the response is completed (no more
    /// bytes) or there is an error.
    virtual std::vector<char>& nextBuffer() { return _buffer; }

    /// Flush the retrieved buffer where bLen bytes were set. If last==true,
    /// then no more buffer() and flush() calls should occur.
    /// @return true if successful (no error)
    virtual bool flush(int bLen, bool last);

    /// Signal an unrecoverable error condition. No further calls are expected.
    virtual void errorFlush(std::string const& msg, int code);

    /// @return true if the receiver has completed its duties.
    virtual bool finished() const;

    virtual bool reset(); ///< Reset the state that a request can be retried.

    /// Print a string representation of the receiver to an ostream
    virtual std::ostream& print(std::ostream& os) const;

    /// @return an error code and description
    virtual Error getError() const {
        boost::lock_guard<boost::mutex> lock(_errorMutex);
        return _error;
    }

    /// Cancel operations on the Receiver.
    /// This cancels internal state and calls _cancelFunc .
    ///
    virtual void cancel();

    using ResponseRequester::registerCancel;

private:
    void _initState();
    bool _merge();
    void _setError(int code, std::string const& msg);
    bool _setResult();
    bool _verifyResult();

    std::shared_ptr<MsgReceiver> _msgReceiver; ///< Message code receiver
    std::shared_ptr<rproc::InfileMerger> _infileMerger; ///< Merging delegate
    std::string _tableName; ///< Target table name
    std::vector<char> _buffer; ///< Raw response buffer, resized for each msg
    Error _error; ///< Error description
    mutable boost::mutex _errorMutex; ///< Protect readers from partial updates
    MsgState _state; ///< Received message state
    std::shared_ptr<proto::WorkerResponse> _response; ///< protobufs msg buf
    bool _flushed; ///< flushed to InfileMerger?
    boost::mutex _cancelledMutex; ///< Protect check/write of cancel flag.
    bool _cancelled; ///< Cancelled?
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_CCONTROL_MERGINGREQUESTER_H
