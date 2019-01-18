// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
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
#include <memory>
#include <ostream>
#include <string>
#include <vector>


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryTemplate;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// ColumnRef is an abstract value class holding a parsed single column ref
class ColumnRef {
public:
    typedef std::shared_ptr<ColumnRef>  Ptr;
    typedef std::vector<Ptr> Vector;

    ColumnRef(std::string db_, std::string table_, std::string column_)
        : db(db_), table(table_), column(column_) {}
    static Ptr newShared(std::string const& db_,
                         std::string const& table_,
                         std::string const& column_) {
        return std::make_shared<ColumnRef>(db_, table_, column_);
    }

    std::string db;
    std::string table;
    std::string column;
    friend std::ostream& operator<<(std::ostream& os, ColumnRef const& cr);
    friend std::ostream& operator<<(std::ostream& os, ColumnRef const* cr);
    void renderTo(QueryTemplate& qt) const;

    // Returns true if the fields in rhs have the same values as the fields in this, without considering
    // unpopulated fields. This can be used to determine if rhs could refer to the same column as this
    // ColumnRef.
    // Only considers populated member variables, e.g. if `db` is not populated in this or in rhs it is
    // ignored during comparison, except if e.g. db is populated but table is not (or table is but column is
    // not) this will return false.
    // This function requires that the column field be populated, and requires that less significant fields
    // be populated if more significant fields are populated, e.g. if db is populated, table (and column)
    // must be populated.
    bool isSubsetOf(const ColumnRef::Ptr & rhs) const;

    bool operator==(const ColumnRef& rhs) const;
    bool operator!=(const ColumnRef& rhs) const { return false == (*this == rhs); }
    bool operator<(const ColumnRef& rhs) const;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_COLUMNREF_H
