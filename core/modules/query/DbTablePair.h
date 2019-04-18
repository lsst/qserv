// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2019 LSST Corporation.
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


#ifndef LSST_QSERV_QUERY_DBTABLEPAIR_H
#define LSST_QSERV_QUERY_DBTABLEPAIR_H


// System headers
#include <ostream>
#include <set>
#include <string>
#include <vector>


namespace lsst {
namespace qserv {
namespace query {

struct DbTablePair {
    DbTablePair(std::string const& db_, std::string const& table_)
        : db(db_), table(table_) {}
    DbTablePair() {}
    bool empty() const { return db.empty() && table.empty(); }
    bool operator<(DbTablePair const& rhs) const {
        if(db < rhs.db) return true;
        else if(db == rhs.db) { return table < rhs.table; }
        else return false;
    }
    bool operator==(DbTablePair const& rhs) const {
        return (db == rhs.db && table == rhs.table);
    }
    std::string db;
    std::string table;
};

inline std::ostream& operator<<(std::ostream& out, DbTablePair const& dbTablePair) {
    return out << std::string("DbTablePair(") << dbTablePair.db << ", " << dbTablePair.table << ")";
}

typedef std::vector<DbTablePair> DbTableVector;
using DbTableSet=std::set<DbTablePair>;


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_DBTABLEPAIR_H
