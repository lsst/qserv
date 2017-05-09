// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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


#ifndef LSST_QSERV_DBTABLE_H
#define LSST_QSERV_DBTABLE_H

// System headers
#include <ostream>
#include <set>
#include <string>


namespace lsst {
namespace qserv {


struct DbTable {
    DbTable(std::string const& db_, std::string const& table_) : db(db_), table(table_) {}
    DbTable(DbTable const& dbtbl) = default;

    DbTable& operator=(DbTable const& dbtbl) = default;
    bool operator<(DbTable const& rhs) const {
        if (db < rhs.db) return true;
        if (table < rhs.table) return true;
        return false;
    }

    std::string db;
    std::string table;

    friend std::ostream& operator<<(std::ostream& os, DbTable const& dbTable);
};

typedef std::set<DbTable> DbTableSet;

}} // namespace lsst::qserv



#endif // LSST_QSERV_DBTABLE_H
