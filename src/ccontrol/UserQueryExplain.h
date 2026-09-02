// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2026 LSST.
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

#ifndef LSST_QSERV_CCONTROL_USERQUERYEXPLAIN_H
#define LSST_QSERV_CCONTROL_USERQUERYEXPLAIN_H

// System headers
#include <memory>
#include <string>

// Third-party headers
#include "nlohmann/json_fwd.hpp"

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "global/intTypes.h"

// Forward decl
namespace lsst::qserv::qproc {
class QuerySession;
}  // namespace lsst::qserv::qproc

namespace lsst::qserv::ccontrol {

/// UserQueryExplain : implementation of UserQuery for `EXPLAIN [FORMAT=JSON] <select>`.
///
/// The supplied QuerySession should have already had analysis run previously. This query type places analysis
/// info into a result table (either tabular or JSON format).
class UserQueryExplain : public UserQuery {
public:
    /**
     *  @param qSession   Already-analyzed QuerySession for the inner SELECT query.
     *  @param userQueryId Unique string identifying query
     *  @param resultDb   Name of the database that will contain results.
     *  @param jsonFormat If true, output a single-column JSON document
     */
    UserQueryExplain(std::shared_ptr<qproc::QuerySession> const& qSession, std::string const& userQueryId,
                     std::string const& resultDb, bool jsonFormat);

    UserQueryExplain(UserQueryExplain const&) = delete;
    UserQueryExplain& operator=(UserQueryExplain const&) = delete;

    /// @return a non-empty string describing the current error state
    std::string getError() const override;

    /// Populate the EXPLAIN result table.
    void submit() override;

    /// Wait until the query has completed execution (everything happens in submit()).
    QueryState join() override;

    /// No-op, does not run async
    void kill() override;

    /// No-op, no resources to release
    void discard() override;

    // Delegate objects
    std::shared_ptr<qmeta::MessageStore> getMessageStore() override { return _messageStore; }

    /// @return Name of the result table for this query.
    std::string getResultTableName() const override { return _resultTableName; }

    /// @return Result location for this query.
    std::string getResultLocation() const override { return "table:" + getResultTableName(); }

    std::string getResultQuery() const override;

private:
    /// Gather query analysis info as JSON, one attribute per key.
    nlohmann::json _getQueryInfo() const;

    /// Create and populate the result table. May throw; callers must not let that escape submit().
    void _populateResultTable();

    std::shared_ptr<qproc::QuerySession> _qSession;
    bool const _jsonFormat;
    QueryState _qState = UNKNOWN;
    std::shared_ptr<qmeta::MessageStore> _messageStore;
    std::string _resultTableName;
    std::string _resultDb;
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_USERQUERYEXPLAIN_H
