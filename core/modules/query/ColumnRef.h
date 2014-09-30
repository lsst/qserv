// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_COLUMNREF_H
#define LSST_QSERV_QUERY_COLUMNREF_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <list>
#include <ostream>
#include <string>

// Third-party headers
#include "boost/shared_ptr.hpp"

namespace lsst {
namespace qserv {
namespace query {

class QueryTemplate; // Forward

/// ColumnRef is an abstract value class holding a parsed single column ref
class ColumnRef {
public:
    typedef boost::shared_ptr<ColumnRef>  Ptr;
    typedef std::list<Ptr> List;

    ColumnRef(std::string db_, std::string table_, std::string column_)
        : db(db_), table(table_), column(column_) {}
    static Ptr newShared(std::string const& db_,
                         std::string const& table_,
                         std::string const& column_) {
        return Ptr(new ColumnRef(db_, table_, column_));
    }

    std::string db;
    std::string table;
    std::string column;
    friend std::ostream& operator<<(std::ostream& os, ColumnRef const& cr);
    friend std::ostream& operator<<(std::ostream& os, ColumnRef const* cr);
    void renderTo(QueryTemplate& qt) const;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_COLUMNREF_H
