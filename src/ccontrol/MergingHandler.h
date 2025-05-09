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

namespace lsst::qserv::qdisp {
class Executive;
class JobQuery;
class MergeEndStatus;
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
    MergingHandler(std::shared_ptr<rproc::InfileMerger> const& merger,
                   std::shared_ptr<qdisp::Executive> const& exec);

    /// @see ResponseHandler::flushHttp
    /// @see MerginHandler::_mergeHttp
    /// @see qdisp::MergeEndStatus
    qdisp::MergeEndStatus flushHttp(std::string const& fileUrl, uint64_t fileSize) override;

    /// @see ResponseHandler::flushHttpError
    void flushHttpError(int errorCode, std::string const& errorMsg, int status) override;

    /// Signal an unrecoverable error condition. No further calls are expected.
    void errorFlush(std::string const& msg, int code) override;

    /// Print a string representation of the receiver to an ostream
    std::ostream& print(std::ostream& os) const override;

private:
    /// Call InfileMerger to do the work of merging this data to the result.
    qdisp::MergeEndStatus _mergeHttp(std::shared_ptr<qdisp::UberJob> const& uberJob,
                                     std::string const& fileUrl, uint64_t fileSize);

    /// Set error code and string.
    void _setError(int code, std::string const& msg, int errorState);

    /// Check if the query is no longer active.
    /// This is used to prevent the query from being processed after it has been cancelled
    /// or finished for any reason.
    /// @param jobQuery the query to check
    /// @return true if the query is no longer active
    bool _queryIsNoLongerActive(std::shared_ptr<qdisp::JobQuery> const& jobQuery) const;

    std::shared_ptr<rproc::InfileMerger> _infileMerger;  ///< Merging delegate
    std::atomic<bool> _errorSet{false};                  ///< Set to true when an error is set.
    bool _flushed{false};                                ///< flushed to InfileMerger?
    std::string _wName{"~"};                             ///< worker name

    std::weak_ptr<qdisp::Executive> _executive;  ///< Weak pointer to the executive for errors.
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_MERGINGHANDLER_H
