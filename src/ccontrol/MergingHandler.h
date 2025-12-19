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

namespace lsst::qserv::mysql {
class CsvMemDisk;
}  // namespace lsst::qserv::mysql

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
/// czar-side knowledge of the worker's response protocol.
/// The czar collects a result file from the worker and merges that into
/// the query result table.
class MergingHandler : public qdisp::ResponseHandler {
public:
    typedef std::shared_ptr<MergingHandler> Ptr;

    enum MergeState { PREMERGE, MERGING, CANCELLED };

    virtual ~MergingHandler();

    /// @param merger downstream merge acceptor
    MergingHandler(std::shared_ptr<rproc::InfileMerger> const& merger,
                   std::shared_ptr<qdisp::Executive> const& exec);

    /// @see ResponseHandler::flushHttp
    /// @see MerginHandler::_mergeHttp
    /// @see qdisp::MergeEndStatus
    qdisp::MergeEndStatus flushHttp(std::string const& fileUrl, std::uint64_t fileSize) override;

    /// Signal an unrecoverable error condition. No further calls are expected.
    void errorFlush(std::string const& msg, int code) override;

    /// Stop an ongoing file merge, if possible.
    /// @return true if the merge was cancelled.
    bool cancelFileMerge() override;

    /// Print a string representation of the receiver to an ostream
    std::ostream& print(std::ostream& os) const override;

private:
    /// Call InfileMerger to do the work of merging this data to the result.
    qdisp::MergeEndStatus _mergeHttp(std::shared_ptr<qdisp::UberJob> const& uberJob,
                                     std::string const& fileUrl, std::uint64_t fileSize);

    /// Set error code and string.
    void _setError(int code, int subCode, std::string const& msg, int errorState);

    /// Return true if merging should be started and set _mergeState to MERGING.
    /// This should only be called once after the file has been collected and
    /// before merging with the result table starts.
    bool _startMerge();

    // All instances of the HTTP client class are members of the same pool. This allows
    // connection reuse and a significant reduction of the kernel memory pressure.
    // Note that the pool gets instantiated at the very first call to method _getHttpConnPool()
    // because the instantiation depends on the availability of the Czar configuration.
    static std::shared_ptr<http::ClientConnPool> const& _getHttpConnPool();
    static std::shared_ptr<http::ClientConnPool> _httpConnPool;
    static std::mutex _httpConnPoolMutex;

    std::shared_ptr<rproc::InfileMerger> _infileMerger;  ///< Merging delegate
    std::atomic<bool> _errorSet{false};                  ///< Set to true when an error is set.
    bool _flushed{false};                                ///< flushed to InfileMerger?
    std::string _wName{"~"};                             ///< worker name

    std::weak_ptr<qdisp::Executive> _executive;    ///< Weak pointer to the executive for errors.
    std::weak_ptr<mysql::CsvMemDisk> _csvMemDisk;  ///< Weak pointer to cancel infile merge.

    /// Indicates merge state of the result table relating to the UberJob associated with
    /// instance of MergingHandler.
    MergeState _mergeState = PREMERGE;
    std::mutex _mergeStateMtx;  ///< Protectes _mergeState
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_MERGINGHANDLER_H
