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

// Forward declarations

namespace lsst::qserv::http {
class ClientConnPool;
}  // namespace lsst::qserv::http

namespace lsst::qserv::proto {
class ResponseData;
class ResponseSummary;
}  // namespace lsst::qserv::proto

namespace lsst::qserv::qdisp {
class JobQuery;
class UberJob;
}  // namespace lsst::qserv::qdisp

namespace lsst::qserv::rproc {
class InfileMerger;
}  // namespace lsst::qserv::rproc

namespace lsst::qserv::ccontrol {

/// MergingHandler is an implementation of a ResponseHandler that implements
/// czar-side knowledge of the worker's response protocol. It leverages XrdSsi's
/// API by pulling the exact number of bytes needed for the next logical
/// fragment instead of performing buffer size and offset
/// management. Fully-constructed protocol messages are then passed towards an
/// InfileMerger.
/// Do to the way the code works, MerginHandler is effectively single threaded.
/// The worker can only send the data for this job back over a single channel
/// and it can only send one transmit on that channel at a time.
class MergingHandler : public qdisp::ResponseHandler {
public:
    typedef std::shared_ptr<MergingHandler> Ptr;
    virtual ~MergingHandler();

    /// @param merger downstream merge acceptor
    /// @param tableName target table for incoming data
    MergingHandler(std::shared_ptr<rproc::InfileMerger> merger, std::string const& tableName);

    /// Process the response and read the result file if no error was reported by a worker.
    /// @return true if successful (no error)
    bool flush(proto::ResponseSummary const& responseSummary, uint32_t& resultRows) override;

    /// &&&uj doc   see ResponseHandler::flushHttp
    /// @return success - true if the operation was successful
    /// @return shouldCancel - if success was false, this being true indicates there
    ///                   was an unrecoverable error in table writing and the query
    ///                   should be cancelled.
    std::tuple<bool, bool> flushHttp(std::string const& fileUrl, uint64_t expectedRows,
                                     uint64_t& resultRows) override;

    /// &&&uj doc
    bool flushHttpError();

    /// Signal an unrecoverable error condition. No further calls are expected.
    void errorFlush(std::string const& msg, int code) override;

    /// @return true if the receiver has completed its duties.
    bool finished() const override;

    bool reset() override;  ///< Reset the state that a request can be retried.

    /// Print a string representation of the receiver to an ostream
    std::ostream& print(std::ostream& os) const override;

    /// @return an error code and description
    Error getError() const override {
        std::lock_guard<std::mutex> lock(_errorMutex);
        return _error;
    }

private:
    /// Prepare for first call to flush().
    void _initState();

    bool _merge(proto::ResponseSummary const& responseSummary, proto::ResponseData const& responseData,
                std::shared_ptr<qdisp::JobQuery> const& jobQuery);

    /// &&&uj doc
    bool _mergeHttp(std::shared_ptr<qdisp::UberJob> const& uberJob, proto::ResponseData const& responseData);

    /// Set error code and string.
    void _setError(int code, std::string const& msg);

    // All instances of the HTTP client class are members of the same pool. This allows
    // connection reuse and a significant reduction of the kernel memory pressure.
    // Note that the pool gets instantiated at the very first call to method _getHttpConnPool()
    // because the instantiation depends on the availability of the Czar configuration.
    static std::shared_ptr<http::ClientConnPool> const& _getHttpConnPool();
    static std::shared_ptr<http::ClientConnPool> _httpConnPool;
    static std::mutex _httpConnPoolMutex;

    std::shared_ptr<rproc::InfileMerger> _infileMerger;  ///< Merging delegate
    std::string _tableName;                              ///< Target table name
    Error _error;                                        ///< Error description
    mutable std::mutex _errorMutex;                      ///< Protect readers from partial updates
    bool _flushed{false};                                ///< flushed to InfileMerger?
    std::string _wName{"~"};                             ///< worker name
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_MERGINGHANDLER_H
