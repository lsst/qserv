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

// Qserv headers
#include "query/TableRef.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryTemplate;
}
namespace sql {
    class ColSchema;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {


/// ColumnRef is an abstract value class holding a parsed single _column ref.
/// Note when setting database, table, and column:
// 1. if db is populated, table must be also.
// 2. if table is populated, column must be also.
// Attempting to set db when table is empty, or attempting to make column empty when table is populated will
// result in a logic_error being thrown.
class ColumnRef {
public:
    typedef std::shared_ptr<ColumnRef>  Ptr;
    typedef std::vector<Ptr> Vector;

    ColumnRef(std::string column_);
    ColumnRef(std::string db_, std::string table_, std::string column_);
    ColumnRef(std::string db_, std::string table_, std::string tableAlias_, std::string column_);
    ColumnRef(std::shared_ptr<TableRef> const& table, std::string const& column);

    // Copying is disallowed unless/until really needed; use `clone` or `copySyntax`
    ColumnRef(ColumnRef const& rhs) = delete;

    static Ptr newShared(std::string const& db_,
                         std::string const& table_,
                         std::string const& column_) {
        return std::make_shared<ColumnRef>(db_, table_, column_);
    }

    Ptr clone() const;

    std::string const& getDb() const { return _tableRef->getDb(); }
    std::string const& getTable() const { return _tableRef->getTable(); }
    std::string const& getColumn() const { return _column; }
    std::string const& getTableAlias() const { return _tableRef->getAlias(); }

    std::shared_ptr<TableRef const> getTableRef() const { return _tableRef; }
    std::shared_ptr<TableRef> getTableRef() { return _tableRef; }

    // Set the database name.
    void setDb(std::string const& db);

    // Set the table name.
    void setTable(std::string const& table);

    // Set the db, table, and table alias name (as indicated in the passed in TableRef).
    void setTable(std::shared_ptr<TableRef> const& tableRef);

    // Reset the contained TableRef object, effectively remove the db, table, and table alias names, as well
    // as any JOIN that the TableRef may contain.
    void resetTable();

    // Set the column name.
    void setColumn(std::string const& column);

    // return true if only the column parameter is set; the db, table, and table alias are empty.
    bool isColumnOnly() const;

    friend std::ostream& operator<<(std::ostream& os, ColumnRef const& cr);
    friend std::ostream& operator<<(std::ostream& os, ColumnRef const* cr);
    void renderTo(QueryTemplate& qt) const;

    // Returns true if the fields in rhs have the same values as the fields in this, without considering
    // unpopulated fields. This can be used to determine if rhs could refer to the same _column as this
    // ColumnRef.
    // Only considers populated member variables, e.g. if the database is not populated in this or in rhs it
    // is ignored during comparison, except if e.g. the database is populated but the table is not (or the
    // table is but the column is not) this will return false.
    bool isSubsetOf(const ColumnRef::Ptr & rhs) const;
    bool isSubsetOf(ColumnRef const& rhs) const;

    // determine if this object is the same as or a less complete description of the passed in object.
    bool isSubsetOf(sql::ColSchema const& columnSchema) const;


    bool isAliasedBy(ColumnRef const& rhs) const;

    // Return true if all the fields are populated, false if a field (like the database field) is empty.
    bool isComplete() const;

    bool operator==(const ColumnRef& rhs) const;
    bool operator!=(const ColumnRef& rhs) const { return false == (*this == rhs); }
    bool operator<(const ColumnRef& rhs) const;

    std::string sqlFragment() const;

private:
    // make sure the current value of the tableRef and column meet stated requirements:
    // 1. if db is populated, table must be also.
    // 2. if table is populated, column must be also.
    void _verify() const;

    // The TableRef in a ColumnRef should always be "simple" (have no joins). Right now this is enforced
    // simply because the only way a TableRef is set here is in the implementation of this class.
    std::shared_ptr<TableRef> _tableRef;
    std::string _column;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_COLUMNREF_H
