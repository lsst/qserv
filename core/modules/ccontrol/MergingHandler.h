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


class MergeBuffer {
public:
    using bufType = std::vector<char>;
    MergeBuffer() { zero(); }
    virtual ~MergeBuffer() { if (_buff != nullptr) _totalBytes -= _buff->size(); }

    std::shared_ptr<bufType> getBuffer() {
        if (_buff == nullptr) zero();
        return _buff;
    }
    size_t getSize() {
        if (_buff == nullptr) return 0;
        return _buff->size();
    }
    void zero();
    void resize(int sz);

private:
    std::shared_ptr<bufType> _buff;
    static std::atomic<long long int> _totalBytes;
};


/// MergingHandler is an implementation of a ResponseHandler that implements
/// czar-side knowledge of the worker's response protocol. It leverages XrdSsi's
/// API by pulling the exact number of bytes needed for the next logical
/// fragment instead of performing buffer size and offset
/// management. Fully-constructed protocol messages are then passed towards an
/// InfileMerger.
class MergingHandler : public qdisp::ResponseHandler {
public:
    /// Possible MergingHandler message state
    enum class MsgState { INVALID, HEADER_SIZE_WAIT,
                    RESULT_WAIT, RESULT_EXTRA,
                    RESULT_RECV, 
                    HEADER_ERR, RESULT_ERR };
    static const char* getStateStr(MsgState const& st);

    typedef std::shared_ptr<MergingHandler> Ptr;
    virtual ~MergingHandler();

    /// @param msgReceiver Message code receiver
    /// @param merger downstream merge acceptor
    /// @param tableName target table for incoming data
    MergingHandler(std::shared_ptr<MsgReceiver> msgReceiver,
                     std::shared_ptr<rproc::InfileMerger> merger,
                     std::string const& tableName);

    /// @return a char vector to receive the next message. The vector
    /// should be sized to the request size. The buffer will be filled
    /// before flush(), unless the response is completed (no more
    /// bytes) or there is an error.
    // std::vector<char>& nextBuffer() override { return *_buffer; } &&&
    std::vector<char>& nextBuffer() override { return *_mBuf.getBuffer(); }

    /// Flush the retrieved buffer where bLen bytes were set. If last==true,
    /// then no more buffer() and flush() calls should occur.
    /// @return true if successful (no error)
    bool flush(int bLen, bool& last, bool& largeResult) override;

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

    /// Scrub the results from jobId-attempt from the result table.
    bool scrubResults(int jobId, int attempt) override;

    //static std::atomic<int> bufferTotal{0}; &&& delete
private:
    void _initState();
    bool _merge();
    void _setError(int code, std::string const& msg);
    bool _setResult();
    bool _verifyResult();

    std::shared_ptr<MsgReceiver> _msgReceiver; ///< Message code receiver
    std::shared_ptr<rproc::InfileMerger> _infileMerger; ///< Merging delegate
    std::string _tableName; ///< Target table name
    // std::shared_ptr<std::vector<char>> _buffer; ///< Raw response buffer, resized for each msg &&& delete
    MergeBuffer _mBuf;
    Error _error; ///< Error description
    mutable std::mutex _errorMutex; ///< Protect readers from partial updates
    MsgState _state; ///< Received message state
    std::shared_ptr<proto::WorkerResponse> _response; ///< protobufs msg buf
    bool _flushed {false}; ///< flushed to InfileMerger?
    std::string _wName {"~"}; /// worker name
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_CCONTROL_MERGINGHANDLER_H
