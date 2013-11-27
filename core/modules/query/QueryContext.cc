/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file QueryContext.cc
  *
  * @brief QueryContext implementation.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "query/QueryContext.h"
#include "query/ColumnRef.h"

namespace lsst {
namespace qserv {
namespace query {

/// Resolve a column ref to a concrete (db,table)
/// @return the concrete (db,table), based on current context.
DbTablePair
QueryContext::resolve(boost::shared_ptr<ColumnRef> cr) {
    if(!cr) { return DbTablePair(); }

    // If alias, retrieve real reference.
    if(cr->db.empty() && !cr->table.empty()) {
        DbTablePair concrete = tableAliases.get(cr->table);
        if(!concrete.empty()) {
            if(concrete.db.empty()) {
                concrete.db = defaultDb;
            }
            return concrete;
        }
    }
    // Set default db and table.
    DbTablePair p(defaultDb, anonymousTable);

    // Extract db and table from ref if available
    if(!cr->db.empty()) { p.db = cr->db; }
    if(!cr->table.empty()) { p.table = cr->table; }
    return p;
}

}}} // namespace lsst::qserv::query
