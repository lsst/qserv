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
#ifndef LSST_QSERV_SQL_STATEMENT_H
#define LSST_QSERV_SQL_STATEMENT_H

// System headers
#include <memory>
#include <string>
#include <vector>

namespace lsst {
namespace qserv {
namespace sql {

struct Schema; // Forward

/// Construct a CREATE TABLE statement according to a table name and
/// a schema.
std::string formCreateTable(std::string const& table, sql::Schema const& s);

/// Helper struct for patching columns to/from hex to workaround MySQL
/// limitations in LOAD DATA INFILE
struct InsertColumn {
    std::string column;
    std::string hexColumn;
};
typedef std::vector<InsertColumn> InsertColumnVector;

/// Construct patch spec from a schema
std::shared_ptr<InsertColumnVector> newInsertColumnVector(Schema const& s);

/// Compose a LOAD DATA INFILE statement
std::string formLoadInfile(std::string const& table,
                           std::string const& virtFile);
/// Compose a LOAD DATA INFILE statement that needs binary patching
std::string formLoadInfile(std::string const& table,
                           std::string const& virtFile,
                           InsertColumnVector const& icv);


}}} // namespace lsst::qserv::sql
#endif // LSST_QSERV_SQL_STATEMENT_H
