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

#ifndef LSST_QSERV_CCONTROL_USERQUERYSELECT_H
#define LSST_QSERV_CCONTROL_USERQUERYSELECT_H
/**
  * @file
  *
  * @brief Umbrella container for user query state
   *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <cstdint>
#include <memory>
#include <mutex>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "css/StripingParams.h"
#include "qmeta/QInfo.h"
#include "qmeta/types.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"

// Forward decl
namespace lsst {
namespace qserv {
namespace qdisp {
class Executive;
class MessageStore;
}
namespace qmeta {
class QMeta;
}
namespace qproc {
class QuerySession;
class SecondaryIndex;
}
namespace rproc {
class InfileMerger;
class InfileMergerConfig;
}}}

namespace lsst {
namespace qserv {

namespace qdisp {
class LargeResultMgr;
}

namespace ccontrol {

/// UserQuerySelect : implementation of the UserQuery for regular SELECT statements.
class UserQuerySelect : public UserQuery {
public:

    UserQuerySelect(std::shared_ptr<qproc::QuerySession> const& qs,
                    std::shared_ptr<qdisp::MessageStore> const& messageStore,
                    std::shared_ptr<qdisp::Executive> const& executive,
                    std::shared_ptr<rproc::InfileMergerConfig> const& infileMergerConfig,
                    std::shared_ptr<qproc::SecondaryIndex> const& secondaryIndex,
                    std::shared_ptr<qmeta::QMeta> const& queryMetadata,
                    qmeta::CzarId czarId,
                    std::shared_ptr<qdisp::LargeResultMgr> const& largeResultMgr,
                    std::string const& errorExtra);

    UserQuerySelect(UserQuerySelect const&) = delete;
    UserQuerySelect& operator=(UserQuerySelect const&) = delete;

    void qMetaRegister();

    // Accessors

    /// @return a non-empty string describing the current error state
    /// Returns an empty string if no errors have been detected.
    virtual std::string getError() const override;

    /// Begin execution of the query over all ChunkSpecs added so far.
    virtual void submit() override;

    /// Wait until the query has completed execution.
    /// @return the final execution state.
    virtual QueryState join() override;

    /// Stop a query in progress (for immediate shutdowns)
    virtual void kill() override;

    /// Release resources related to user query
    virtual void discard() override;

    // Delegate objects
    virtual std::shared_ptr<qdisp::MessageStore> getMessageStore() override {
        return _messageStore; }

    /// @return Name of the result table for this query, can be empty
    virtual std::string getResultTableName() override { return _resultTable; }

    /// @return ORDER BY part of SELECT statement to be executed by proxy
    virtual std::string getProxyOrderBy() override;

    virtual std::string getQueryIdString() const override;

    /* &&& unused
    /// Add a chunk for later execution
    void addChunk(qproc::ChunkSpec const& cs);
    */

    void setupChunking();

private:
    void _setupMerger();
    void _discardMerger();
    void _qMetaUpdateStatus(qmeta::QInfo::QStatus qStatus);
    void _qMetaAddChunks(std::vector<int> const& chunks);
    /* &&&
    void _sendToWorker(qproc::ChunkSpec const& chunkSpec, std::mutex& mtx, std::vector<int>& chunks,
            proto::ProtoImporter<proto::TaskMsg>& pi,
            int& msgCount, int sequence); // &&& cleanup
    */

    // Delegate classes
    std::shared_ptr<qproc::QuerySession> _qSession;
    std::shared_ptr<qdisp::MessageStore> _messageStore;
    std::shared_ptr<qdisp::Executive> _executive;
    std::shared_ptr<rproc::InfileMergerConfig> _infileMergerConfig;
    std::shared_ptr<rproc::InfileMerger> _infileMerger;
    std::shared_ptr<qproc::SecondaryIndex> _secondaryIndex;
    std::shared_ptr<qmeta::QMeta> _queryMetadata;

    qmeta::CzarId _qMetaCzarId; ///< Czar ID in QMeta database
    QueryId _qMetaQueryId{0};      ///< Query ID in QMeta database
    std::shared_ptr<qdisp::LargeResultMgr> _largeResultMgr;
    /// QueryId in a standard string form, initially set to unknown.
    std::string _queryIdStr{QueryIdHelper::makeIdStr(0, true)};
    bool _killed{false};
    std::mutex _killMutex;
    std::string _errorExtra;    ///< Additional error information
    std::string _resultTable;   ///< Result table name
};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYSELECT_H
