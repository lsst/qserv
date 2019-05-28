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
    class TableRefBase;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// ColumnRef is an abstract value class holding a parsed single _column ref
class ColumnRef {
public:
    typedef std::shared_ptr<ColumnRef>  Ptr;
    typedef std::vector<Ptr> Vector;

    ColumnRef(std::string db_, std::string table_, std::string column_);
    ColumnRef(std::string db_, std::string table_, std::string tableAlias_, std::string column_);
    ColumnRef(std::shared_ptr<TableRefBase> const& table, std::string const& column);
    static Ptr newShared(std::string const& db_,
                         std::string const& table_,
                         std::string const& column_) {
        return std::make_shared<ColumnRef>(db_, table_, column_);
    }

    std::string const& getDb() const;
    std::string const& getTable() const;
    std::string const& getColumn() const;
    std::string const& getTableAlias() const;

    std::shared_ptr<TableRefBase> getTableRef() const;
    std::shared_ptr<TableRefBase>& getTableRef();

    void setDb(std::string const& db);
    void setTable(std::string const& table);
    void setColumn(std::string const& column);
    void set(std::string const& db, std::string const& table, std::string const& column);

    friend std::ostream& operator<<(std::ostream& os, ColumnRef const& cr);
    friend std::ostream& operator<<(std::ostream& os, ColumnRef const* cr);
    void renderTo(QueryTemplate& qt) const;

    // Returns true if the fields in rhs have the same values as the fields in this, without considering
    // unpopulated fields. This can be used to determine if rhs could refer to the same _column as this
    // ColumnRef.
    // Only considers populated member variables, e.g. if the database is not populated in this or in rhs it
    // is ignored during comparison, except if e.g. the database is populated but the table is not (or the
    // table is but the column is not) this will return false.
    // This function requires that the the column field be populated, and requires that less significant
    // fields be populated if more significant fields are populated, e.g. if the database is populated, the
    // table (and the column) must be populated.
    bool isSubsetOf(const ColumnRef::Ptr & rhs) const;

    bool isAliasedBy(ColumnRef const& rhs) const;

    // Compare this ColumnRef to rhs and return true if it is less than the other.
    // This will consider the alias, so if this == rhs by the alias it will return false.
    // That is, "db.table.col AS a" will be equal to "a.col"
    bool lessThan(ColumnRef const& rhs, bool useAlias) const;
    bool equal(ColumnRef const& rhs, bool useAlias) const;

    bool operator==(const ColumnRef& rhs) const;
    bool operator!=(const ColumnRef& rhs) const { return false == (*this == rhs); }
    bool operator<(const ColumnRef& rhs) const;

private:
    std::shared_ptr<TableRefBase> _tableRef;
    std::string _column;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_COLUMNREF_H
