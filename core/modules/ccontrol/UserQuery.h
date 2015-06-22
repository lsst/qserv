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

#ifndef LSST_QSERV_CCONTROL_USERQUERY_H
#define LSST_QSERV_CCONTROL_USERQUERY_H
/**
  * @file
  *
  * @brief Umbrella container for user query state
   *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>
#include <mutex>

// Third-party headers
#include "boost/utility.hpp"

// Qserv headers
#include "ccontrol/QueryState.h"
#include "css/StripingParams.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"

// Forward decl
namespace lsst {
namespace qserv {
namespace qdisp {
class Executive;
class MessageStore;
}
namespace qproc {
class QuerySession;
class SecondaryIndex;
}
namespace rproc {
class TableMerger;
class TableMergerConfig;
class InfileMerger;
class InfileMergerConfig;
}}}

namespace lsst {
namespace qserv {
namespace ccontrol {
class UserQueryFactory;

/// UserQuery : top-level class for user query data. Not thread-safe, although
/// its delegates are thread-safe as appropriate.
class UserQuery : public boost::noncopyable {
public:
    typedef std::shared_ptr<UserQuery> Ptr;
    friend class UserQueryFactory;

    // Accessors

    /// @return a non-empty string describing the current error state
    /// Returns an empty string if no errors have been detected.
    std::string const& getError() const;

    /// Add a chunk for later execution
    void addChunk(qproc::ChunkSpec const& cs);

    /// Begin execution of the query over all ChunkSpecs added so far.
    void submit();

    /// Wait until the query has completed execution.
    /// @return the final execution state.
    QueryState join();

    /// Stop a query in progress (for immediate shutdowns)
    void kill();

    /// Release resources related to user query
    void discard();

    // Delegate objects
    std::shared_ptr<qdisp::Executive> getExecutive() {
        return _executive; }
    std::shared_ptr<qdisp::MessageStore> getMessageStore() {
        return _messageStore; }

private:
    explicit UserQuery(std::shared_ptr<qproc::QuerySession> qs);
    void setSessionId(int session) { _sessionId = session; }
    void _setupMerger();
    void _discardMerger();
    void _setupChunking();

    // Delegate classes
    std::shared_ptr<qdisp::Executive> _executive;
    std::shared_ptr<qdisp::MessageStore> _messageStore;
    std::shared_ptr<qproc::QuerySession> _qSession;
    std::shared_ptr<rproc::InfileMergerConfig> _infileMergerConfig;
    std::shared_ptr<rproc::InfileMerger> _infileMerger;
    std::shared_ptr<qproc::SecondaryIndex> _secondaryIndex;

    bool _killed;
    std::mutex _killMutex;
    int _sessionId; ///< External reference number
    int _sequence; ///< Sequence number for subtask ids
    std::string _errorExtra; ///< Additional error information
    mutable std::string _errorExtraCache; ///< Cache so getError can return a ref
};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERY_H
