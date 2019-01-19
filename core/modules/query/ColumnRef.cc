// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
/**
  * @file
  *
  * @brief class ColumnRef implementation
  *
  * @author Daniel L. Wang, SLAC
  */


// Class header
#include "query/ColumnRef.h"

// System headers
#include <iostream>
#include <tuple>

// Qserv headers
#include "query/QueryTemplate.h"


namespace lsst {
namespace qserv {
namespace query {


std::ostream& operator<<(std::ostream& os, ColumnRef const& cr) {
    os << "ColumnRef(";
    os << "db:" << cr.db;
    os << ", table:" << cr.table;
    os << ", column:" << cr.column;
    os << ")";
    return os;
}


std::ostream& operator<<(std::ostream& os, ColumnRef const* cr) {
    if (nullptr == cr) {
        os << "nullptr";
    } else {
        return os << *cr;
    }
    return os;
}


void ColumnRef::renderTo(QueryTemplate& qt) const {
    qt.append(*this);
}


bool ColumnRef::isSubsetOf(const ColumnRef::Ptr & rhs) const {
    // the columns can not be empty
    if (column.empty() || rhs->column.empty()) {
        return false;
    }
    // if the table is empty, the db must be empty
    if (table.empty() && !db.empty()) {
        return false;
    }
    if (rhs->table.empty() && !rhs->db.empty()) {
        return false;
    }

    if (!db.empty()) {
        if (db != rhs->db) {
            return false;
        }
    }
    if (!table.empty()) {
        if (table != rhs->table) {
            return false;
        }
    }
    if (column != rhs->column) {
        return false;
    }
    return true;
}


bool ColumnRef::operator==(const ColumnRef& rhs) const {
    return std::tie(db, table, column) == std::tie(rhs.db, rhs.table, rhs.column);
}


bool ColumnRef::operator<(const ColumnRef& rhs) const {
    return std::tie(db, table, column) < std::tie(rhs.db, rhs.table, rhs.column);
}


}}} // namespace lsst::qserv::query
