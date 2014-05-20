// -*- LSST-C++ -*-
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

#ifndef LSST_QSERV_QUERY_TABLEALIAS_H
#define LSST_QSERV_QUERY_TABLEALIAS_H
/**
  * @file
  *
  * @brief DbTablePair, TableAlias, and TableAliasReverse declarations.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <sstream>
#include <stdexcept>

// Local headers
#include "query/DbTablePair.h"

namespace lsst {
namespace qserv {
namespace query {

/// TableAlias is a mapping from an alias to a (db, table)
/// name. TableAlias is a forward mapping, and TableAliasReverse is a
/// backwards mapping.
class TableAlias {
public:

    query::DbTablePair get(std::string const& alias) {
        Map::const_iterator i = _map.find(alias);
        if(i != _map.end()) { return i->second; }
        return query::DbTablePair();
    }
    void set(std::string const& db, std::string const& table,
             std::string const& alias) {
        _map[alias] = query::DbTablePair(db, table);
    }
private:
    typedef std::map<std::string, query::DbTablePair> Map;
    Map _map;
};

/// Stores a reverse alias mapping:  (db,table) -> alias
class TableAliasReverse {
public:
    struct AmbiguousReference : public std::runtime_error {
        AmbiguousReference(query::DbTablePair const& p)
            : std::runtime_error("Ambiguous reference to " +
                                 p.db + "." + p.table)
            {}
    };

    std::string const& get(std::string db, std::string table) {
        return get(query::DbTablePair(db, table));
    }
    inline std::string const& get(query::DbTablePair const& p) const {
        static std::string const empty;
        typedef Map::const_iterator Iter;
        Iter found = _map.find(p);

        // Try slow lookup for inexact search
        bool exists = false;
        if((found == _map.end()) && p.db.empty()) {
            for(Iter i=_map.begin(), e=_map.end(); i != e; ++i) {
                if((i->first.table == p.table) && (!i->second.empty())) {
                    if(!exists) {
                        exists = true;
                        found = i;
                        // Continue looking to find other candidates
                    } else { // More than one candidate found, bail out.
                        throw AmbiguousReference(p);
                    }
                }
            }
        }
        if(found != _map.end()) {
            return found->second;
        } else {
            return empty;
        }
    }
    void set(std::string const& db, std::string const& table,
             std::string const& alias) {
        if(alias.empty()) {
            throw std::invalid_argument("Empty mapping");
        }
        _map[query::DbTablePair(db, table)] = alias;
    }
private:
    typedef std::map<query::DbTablePair, std::string> Map;
    Map _map;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_TABLEALIAS_H
