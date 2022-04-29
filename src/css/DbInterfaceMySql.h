// -*- LSST-C++ -*-
/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef LSST_QSERV_CSS_DBINTERFACEMYSQL_H
#define LSST_QSERV_CSS_DBINTERFACEMYSQL_H

// System headers
#include <mutex>
#include <set>
#include <string>

// Third-party headers

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnection.h"

namespace lsst { namespace qserv { namespace css {

/// This class is used to connect to tabular data in a mysql database.
class DbInterfaceMySql {
public:
    /// @param mysqlConf: Configuration object for mysql connection
    explicit DbInterfaceMySql(mysql::MySqlConfig const& mysqlConf);

    DbInterfaceMySql(DbInterfaceMySql const&) = delete;
    DbInterfaceMySql& operator=(DbInterfaceMySql const&) = delete;

    // Destructor
    ~DbInterfaceMySql() = default;

    static std::string getEmptyChunksTableName(std::string const& dbName) { return dbName + "_EmptyChunks"; }

    static std::string getEmptyChunksSchema(std::string const& dbName) {
        return "CREATE TABLE " + getEmptyChunksTableName(dbName) +
               " (chunkId INT NOT NULL PRIMARY KEY) ENGINE = INNODB";
    }

    /// @return a set of empty chunks for database 'dbName'
    //  @throws CssError if the table cannot be opened.
    std::set<int> getEmptyChunks(std::string const& dbName);

private:
    std::shared_ptr<sql::SqlConnection> _conn;
    std::mutex _dbMutex;  ///< Synchronizes access to certain DB operations
};

}}}  // namespace lsst::qserv::css

#endif  // LSST_QSERV_CSS_DBINTERFACEMYSQL_H
