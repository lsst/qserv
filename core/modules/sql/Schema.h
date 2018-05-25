// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#ifndef LSST_QSERV_SQL_SCHEMA_H
#define LSST_QSERV_SQL_SCHEMA_H

/// Schema.h contains ColType, ColSchema, Schema definitions.
/// They are dumb value classes
/// used to represent a SQL table schemata.

// System headers
#include <ostream>
#include <string>
#include <vector>

namespace lsst {
namespace qserv {
namespace sql {

/// Type information for a single column
struct ColType {

    std::string sqlType; ///< Typespec to use in CREATE TABLE
    int mysqlType; ///< Internal MYSQL type code
};

inline std::ostream& operator<<(std::ostream& os, ColType const& ct) {
    os << ct.sqlType;
    return os;
}

/// Schema for a single column
struct ColSchema {
    std::string name; ///< Column name
    ColType colType; ///< Column type
};

// Related ColSchema types for convenience
typedef std::vector<ColSchema> ColSchemaVector;
typedef ColSchemaVector::const_iterator ColumnsIter;

inline std::ostream& operator<<(std::ostream& os, ColSchema const& cs) {
    os << "`" << cs.name << "` " << cs.colType;
    return os;
}

/// A SQL Table schema.
/// If we end up needing additional characteristics, such as ENGINE or KEY or
/// INDEX, we would add those fields to this struct.
struct Schema {
    ColSchemaVector columns;
};

}}} // namespace lsst::qserv::sql
#endif // LSST_QSERV_SQL_SCHEMA_H
