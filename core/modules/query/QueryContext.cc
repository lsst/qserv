/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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

// Local headers
#include "query/ColumnRef.h"

using lsst::qserv::query::DbTablePair;
using lsst::qserv::query::DbTableVector;

namespace lsst {
namespace qserv {
namespace query {

/// Resolve a column ref to a concrete (db,table)
/// @return the concrete (db,table), based on current context.
query::DbTablePair
QueryContext::resolve(boost::shared_ptr<ColumnRef> cr) {
    if(!cr) { return query::DbTablePair(); }

    // If alias, retrieve real reference.
    if(cr->db.empty() && !cr->table.empty()) {
        query::DbTablePair concrete = tableAliases.get(cr->table);
        if(!concrete.empty()) {
            if(concrete.db.empty()) {
                concrete.db = defaultDb;
            }
            return concrete;
        }
    }
    // Set default db and table.
    DbTablePair p;
    if(cr->table.empty()) { // No db or table: choose first resolver pair
        p = resolverTables[0];
        // TODO: We can be fancy and check the column name against the
        // schema for the entries on the resolverTables, and choose
        // the matching entry.
    } else if(cr->db.empty()) { // Table, but not alias.
        // Match against resolver stack
        for(DbTableVector::const_iterator i=resolverTables.begin(),
                e=resolverTables.end();
            i != e; ++i) {
            if(i->table == cr->table) {
                p = *i;
                break;
            }
        }
        return DbTablePair(); // No resolution.
    } else { // both table and db exist, so return them
        return DbTablePair(cr->db, cr->table);
    }
    if(p.db.empty()) {
        // Fill partially-resolved empty db with user db context
        p.db = defaultDb;
    }
    return p;
}

}}} // namespace lsst::qserv::query
