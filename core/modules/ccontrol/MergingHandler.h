// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_CCONTROL_MERGINGHANDLER_H
#define LSST_QSERV_CCONTROL_MERGINGHANDLER_H

// System headers
#include <atomic>
#include <memory>
#include <mutex>
#include <set>

// Qserv headers
#include "qdisp/ResponseHandler.h"

// Forward decl
namespace lsst {
namespace qserv {
  class MsgReceiver;
namespace proto {
  struct WorkerResponse;
}
namespace rproc {
  class InfileMerger;
}}}

namespace lsst {
namespace qserv {
namespace ccontrol {


/// MergingHandler is an implementation of a ResponseHandler that implements
/// czar-side knowledge of the worker's response protocol. It leverages XrdSsi's
/// API by pulling the exact number of bytes needed for the next logical
/// fragment instead of performing buffer size and offset
/// management. Fully-constructed protocol messages are then passed towards an
/// InfileMerger.
class MergingHandler : public qdisp::ResponseHandler {
public:
    /// Possible MergingHandler message state
    enum class MsgState { INVALID,  // &&& delete INVALID
                    HEADER_SIZE_WAIT,
                    RESULT_WAIT,
                    RESULT_EXTRA, // &&& delete
                    RESULT_RECV,  // &&& delete
                    HEADER_ERR, RESULT_ERR }; // &&& change to TRANSMIT_ERR
    static const char* getStateStr(MsgState const& st);

    typedef std::shared_ptr<MergingHandler> Ptr;
    virtual ~MergingHandler();

    /// @param msgReceiver Message code receiver
    /// @param merger downstream merge acceptor
    /// @param tableName target table for incoming data
    MergingHandler(std::shared_ptr<MsgReceiver> msgReceiver,
                     std::shared_ptr<rproc::InfileMerger> merger,
                     std::string const& tableName);

    /// Flush the retrieved buffer where bLen bytes were set. If last==true,
    /// then no more buffer() and flush() calls should occur.
    /// @return true if successful (no error)
    bool flush(int bLen, BufPtr const& bufPtr, bool& last, bool& largeResult,
               int& nextBufSize) override;

    /// Signal an unrecoverable error condition. No further calls are expected.
    void errorFlush(std::string const& msg, int code) override;

    /// @return true if the receiver has completed its duties.
    bool finished() const override;

    bool reset() override; ///< Reset the state that a request can be retried.

    /// Print a string representation of the receiver to an ostream
    std::ostream& print(std::ostream& os) const override;

    /// @return an error code and description
    Error getError() const override {
        std::lock_guard<std::mutex> lock(_errorMutex);
        return _error;
    }


    /// Prepare to scrub the results from jobId-attempt from the result table.
    void prepScrubResults(int jobId, int attempt) override;

private:
    void _initState();
    //&&&bool _merge(bool last);
    bool _merge();
    void _setError(int code, std::string const& msg);
    bool _setResult(BufPtr const& bufPtr, int blen);
    bool _verifyResult(BufPtr const& bufPtr, int blen);


    std::shared_ptr<MsgReceiver> _msgReceiver; ///< Message code receiver
    std::shared_ptr<rproc::InfileMerger> _infileMerger; ///< Merging delegate
    std::string _tableName; ///< Target table name
    Error _error; ///< Error description
    mutable std::mutex _errorMutex; ///< Protect readers from partial updates
    MsgState _state; ///< Received message state
    std::shared_ptr<proto::WorkerResponse> _response; ///< protobufs msg buf
    bool _flushed {false}; ///< flushed to InfileMerger?
    std::string _wName {"~"}; ///< worker name
    std::mutex _setResultMtx; //< Allow only one call to ParseFromArray at a time from _seResult.
    std::set<int> _jobIds; ///< Set of jobIds added in this request.
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_CCONTROL_MERGINGHANDLER_H
