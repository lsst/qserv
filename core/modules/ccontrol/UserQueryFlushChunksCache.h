/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_CCONTROL_USERQUERYFLUSHCHUNKSCACHE_H
#define LSST_QSERV_CCONTROL_USERQUERYFLUSHCHUNKSCACHE_H

// System headers
#include <memory>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"

// Forward decl
namespace lsst {
namespace qserv {
namespace css {
class CssAccess;
}
namespace sql {
class SqlConnection;
}}}

namespace lsst {
namespace qserv {
namespace ccontrol {

/// @addtogroup ccontrol

/**
 *  @ingroup ccontrol
 *
 *  @brief Implementation of UserQuery for FLUSH QSERV_CHUNKS_CACHE.
 */

class UserQueryFlushChunksCache : public UserQuery {
public:

    /**
     *  @param css:           CSS interface
     *  @param dbName:        Name of the database where table is
     *  @param resultDbConn:  Connection to results database
     *  @param resultTable:   Name of the table for query results
     */
    UserQueryFlushChunksCache(std::shared_ptr<css::CssAccess> const& css,
                              std::string const& dbName,
                              sql::SqlConnection* resultDbConn,
                              std::string const& resultTable);

    UserQueryFlushChunksCache(UserQueryFlushChunksCache const&) = delete;
    UserQueryFlushChunksCache& operator=(UserQueryFlushChunksCache const&) = delete;

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

    /// @return ORDER BY part of SELECT statement to be executed by proxy
    virtual std::string getProxyOrderBy() override { return std::string(); }

protected:

private:

    std::shared_ptr<css::CssAccess> const _css;
    std::string const _dbName;
    sql::SqlConnection* _resultDbConn;
    std::string const _resultTable;
    QueryState _qState;
    std::shared_ptr<qdisp::MessageStore> _messageStore;

};

}}} // namespace lsst::qserv::ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYFLUSHCHUNKSCACHE_H
