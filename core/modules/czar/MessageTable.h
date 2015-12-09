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
#ifndef LSST_QSERV_CZAR_MESSAGETABLE_H
#define LSST_QSERV_CZAR_MESSAGETABLE_H

// System headers
#include <memory>
#include <string>

// Third-party headers

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "global/stringTypes.h"
#include "mysql/MySqlConfig.h"


namespace lsst {
namespace qserv {
namespace sql {
class SqlConnection;
}}}

namespace lsst {
namespace qserv {
namespace czar {

/// @addtogroup czar

/**
 *  @ingroup czar
 *
 *  @brief Class representing message table in results database.
 *
 */

class MessageTable  {
public:

    // Constructor takes table name including database name
    explicit MessageTable(std::string const& tableName, mysql::MySqlConfig const& resultConfig);

    /// Create and lock the table
    void lock();

    /// Release lock on message table so that proxy can proceed
    void unlock(ccontrol::UserQuery::Ptr const& userQuery);

protected:

private:

    /// store all messages from current session to the table
    void _saveQueryMessages(ccontrol::UserQuery::Ptr const& userQuery);

    std::string const _tableName;
    std::shared_ptr<sql::SqlConnection> _sqlConn;

};

}}} // namespace lsst::qserv::czar

#endif // LSST_QSERV_CZAR_MESSAGETABLE_H
