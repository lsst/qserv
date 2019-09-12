// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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


#ifndef LSST_QSERV_CCONTROL_USERQUERYQSERVMANAGER_H
#define LSST_QSERV_CCONTROL_USERQUERYQSERVMANAGER_H


// System headers
#include <memory>
#include <string>
#include <vector>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "ccontrol/UserQueryResources.h"
#include "ccontrol/UserQueryError.h"
#include "ccontrol/QueryState.h"
#include "global/intTypes.h"


namespace lsst {
namespace qserv {
namespace qdisp {
    class MessageStore;
}}}


namespace lsst {
namespace qserv {
namespace ccontrol {


// UserQueryQservManager is for handling queries with the form `CALL QSERV_MANAGER("...")`
class UserQueryQservManager : public UserQuery {
public:

    UserQueryQservManager(std::shared_ptr<UserQueryResources> const& queryResources, std::string const& value);

    ~UserQueryQservManager() override = default;

    /// @return a non-empty string describing the current error state
    /// Returns an empty string if no errors have been detected.
    std::string getError() const override { return std::string(); }

    /// Begin execution of the query over all ChunkSpecs added so far.
    void submit() override;

    /// Wait until the query has completed execution.
    /// @return the final execution state.
    QueryState join() override { return _qState; }

    /// Stop a query in progress (for immediate shutdowns)
    void kill() override {}

    /// Release resources related to user query
    void discard() override {}

    // Delegate objects
    std::shared_ptr<qdisp::MessageStore> getMessageStore() override { return _messageStore; }

    std::string getResultLocation() const override { return "table:" + _resultTableName; }

    /// @return get the SELECT statement to be executed by proxy
    virtual std::string getResultQuery() const;

private:
    std::string _value;
    std::string _resultTableName;
    std::shared_ptr<qdisp::MessageStore> _messageStore;
    std::shared_ptr<sql::SqlConnection> _resultDbConn;
    QueryState _qState{UNKNOWN};
    std::string _resultDb;
};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYQSERVMANAGER_H
