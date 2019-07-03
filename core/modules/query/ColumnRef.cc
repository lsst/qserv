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

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/QueryTemplate.h"
#include "query/TableRef.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.query.ColumnRef");

}


namespace lsst {
namespace qserv {
namespace query {


std::ostream& operator<<(std::ostream& os, ColumnRef const& cr) {
    os << "ColumnRef(";
    os << "\"" << *cr._tableRef << "\"";
    os << ", \"" << cr._column << "\"";
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


ColumnRef::ColumnRef(std::string column_)
    : _tableRef(std::make_shared<TableRef>()), _column(column_) {
}


ColumnRef::ColumnRef(std::string db_, std::string table_, std::string column_)
    : _tableRef(std::make_shared<TableRef>(db_, table_, "")), _column(column_) {
}


ColumnRef::ColumnRef(std::string db_, std::string table_, std::string tableAlias_, std::string column_)
    : _tableRef(std::make_shared<TableRef>(db_, table_, tableAlias_)), _column(column_) {
}


ColumnRef::ColumnRef(std::shared_ptr<TableRef> const& table, std::string const& column)
    : _tableRef(table), _column(column) {
}


std::string const& ColumnRef::getDb() const {
    return _tableRef->getDb();
}


std::string const& ColumnRef::getTable() const {
    return _tableRef->getTable();
}


std::string const& ColumnRef::getColumn() const {
    return _column;
}


std::string const& ColumnRef::getTableAlias() const {
    return _tableRef->getAlias();
}


std::shared_ptr<TableRef const> ColumnRef::getTableRef() const {
    return _tableRef;
}


std::shared_ptr<TableRef> ColumnRef::getTableRef() {
    return _tableRef;
}


void ColumnRef::setDb(std::string const& db) {
    LOGS(_log, LOG_LVL_TRACE, *this << "; set db:" << db);
    _tableRef->setDb(db);
}


void ColumnRef::setTable(std::string const& table) {
    LOGS(_log, LOG_LVL_TRACE, *this << "; set table:" << table);
    _tableRef->setTable(table);
}


void ColumnRef::setTable(std::shared_ptr<TableRef> const& tableRef) {
    LOGS(_log, LOG_LVL_TRACE, *this << "; set table:" << *tableRef);
    if (not tableRef->isSimple()) {
        throw std::logic_error("The TableRef used by a ColumnRef must not have any joins.");
    }
    _tableRef = tableRef;
}


void ColumnRef::setColumn(std::string const& column) {
    LOGS(_log, LOG_LVL_TRACE, *this << "; set column:" << column);
    _column = column;
}


void ColumnRef::set(std::string const& db, std::string const& table, std::string const& column) {
    setDb(db);
    setTable(table);
    setColumn(column);
}


void ColumnRef::renderTo(QueryTemplate& qt) const {
    qt.append(*this);
}


bool ColumnRef::isSubsetOf(const ColumnRef::Ptr & rhs) const {
    return isSubsetOf(*rhs);
}


bool ColumnRef::isColumnOnly() const {
    if (_tableRef->hasDb() || _tableRef->hasTable() || _tableRef->hasAlias()) {
        return false;
    }
    return true;
}


bool ColumnRef::isSubsetOf(ColumnRef const& rhs) const {
    if (not _tableRef->isSubsetOf(*rhs._tableRef)) {
        return false;
    }

    // the columns can not be empty
    if (_column.empty() || rhs._column.empty()) {
        return false;
    }
    if (_column != rhs._column) {
        return false;
    }
    return true;
}


bool ColumnRef::isAliasedBy(ColumnRef const& rhs) const {
    if (_column != rhs._column) {
        return false;
    }
    return _tableRef->isAliasedBy(*rhs._tableRef);
}


bool ColumnRef::isComplete() const {
    if (_column.empty()) // should not be possible, but for completeness we check it.
        return false;
    return _tableRef->isComplete();
}


std::string ColumnRef::sqlFragment() const {
    QueryTemplate qt;
    renderTo(qt);
    return boost::lexical_cast<std::string>(qt);
}


bool ColumnRef::operator==(const ColumnRef& rhs) const {
    return std::tie(*_tableRef, _column) == std::tie(*rhs._tableRef, rhs._column);
}


bool ColumnRef::operator<(const ColumnRef& rhs) const {
    return std::tie(*_tableRef, _column) < std::tie(*rhs._tableRef, rhs._column);
}


}}} // namespace lsst::qserv::query
