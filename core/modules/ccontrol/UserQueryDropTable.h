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

#ifndef LSST_QSERV_CCONTROL_USERQUERYDROPTABLE_H
#define LSST_QSERV_CCONTROL_USERQUERYDROPTABLE_H

// System headers
#include <memory>
#include <mutex>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "qmeta/types.h"

// Forward decl
namespace lsst {
namespace qserv {
namespace css {
class CssAccess;
}
namespace qmeta {
class QMeta;
}
namespace sql {
class SqlConnection;
}}}


namespace lsst {
namespace qserv {
namespace ccontrol {

/// UserQueryDropTable : implementation of the UserQuery for regular SELECT statements.
class UserQueryDropTable : public UserQuery {
public:

    /**
     *  @param css:           CSS interface
     *  @param dbName:        Name of the database where table is
     *  @param tableName:     Name of the table to drop
     *  @param resultDbConn:  Connection to results database
     *  @param resultTable:   Name of the table for query results
     *  @param queryMetadata: QMeta interface
     *  @param qMetaCzarId:   Czar ID in QMeta database
     */
    UserQueryDropTable(std::shared_ptr<css::CssAccess> const& css,
                       std::string const& dbName,
                       std::string const& tableName,
                       sql::SqlConnection* resultDbConn,
                       std::string const& resultTable,
                       std::shared_ptr<qmeta::QMeta> const& queryMetadata,
                       qmeta::CzarId qMetaCzarId);

    UserQueryDropTable(UserQueryDropTable const&) = delete;
    UserQueryDropTable& operator=(UserQueryDropTable const&) = delete;

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

    void setSessionId(int session) { _sessionId = session; }

private:

    std::shared_ptr<css::CssAccess> const _css;
    std::string const _dbName;
    std::string const _tableName;
    sql::SqlConnection* _resultDbConn;
    std::string const _resultTable;
    std::shared_ptr<qmeta::QMeta> _queryMetadata;
    qmeta::CzarId const _qMetaCzarId;   ///< Czar ID in QMeta database
    QueryState _qState;
    std::shared_ptr<qdisp::MessageStore> _messageStore;
    int _sessionId; ///< External reference number

};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYDROPTABLE_H
