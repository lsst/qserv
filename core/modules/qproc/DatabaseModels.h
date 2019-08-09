/*
 * LSST Data Management System
 * Copyright 2019 LSST.
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
#ifndef LSST_QSERV_QPROC_DATABASEMODELS_H
#define LSST_QSERV_QPROC_DATABASEMODELS_H

// System headers
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// Third-party headers

// LSST headers

// Qserv headers
#include "sql/SqlConfig.h"
#include "sql/SqlConnection.h"


namespace lsst {
namespace qserv {
namespace qproc {

/// This class allows access to model versions of the databases kept in qserv. The models
/// are empty databases that have the same schema as the databases in qserv and the models are not sharded.
class DatabaseModels {
public:
    using Ptr = std::shared_ptr<DatabaseModels>;

    DatabaseModels() = delete;
    DatabaseModels(DatabaseModels const&) = delete;
    DatabaseModels& operator=(DatabaseModels const&) = delete;

    virtual ~DatabaseModels() = default;

    /// @return a DatabaseModels object from config (see util::configStor)
    static Ptr create(std::map<std::string, std::string> const& config);

    /// @return a DatabaseModels object from a sql::SqlConfig
    static Ptr create(sql::SqlConfig const& cfg);

    /// Apply a sql statement 'sql' to the database behind DatabaseModels, putting the result
    /// in 'results' and errors in 'errObj'
    bool applySql(std::string const& sql, sql::SqlResults& results, sql::SqlErrorObject& errObj);

    /// @return a list of column names for 'tableName' in database 'dbName'.
    std::vector<std::string> listColumns(std::string const& dbName, std::string const& tableName);

private:
    explicit DatabaseModels(sql::SqlConfig const& sqlConfig);

    std::shared_ptr<sql::SqlConnection> _conn;
    std:: mutex _sqlMutex; ///< protects _conn
};

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_DATABASEMODELS_H
