// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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
  * @brief Implementation of a SelectStmt
  *
  * SelectStmt is the query info structure. It contains information
  * about the top-level query characteristics. It shouldn't contain
  * information about run-time query execution.  It might contain
  * enough information to generate queries for execution.
  *
  * @author Daniel L. Wang, SLAC
  */


// Class header
#include "query/SelectStmt.h"

// System headers
#include <map>
#include <sstream>

// Third-party headers
#include "boost/algorithm/string/predicate.hpp" // string iequal

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/FromList.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/OrderByClause.h"
#include "query/SelectList.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "util/IterableFormatter.h"
#include "util/PointerCompare.h"


////////////////////////////////////////////////////////////////////////
// anonymous
////////////////////////////////////////////////////////////////////////
namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.query.SelectStmt");

template <typename T>
inline void renderTemplate(lsst::qserv::query::QueryTemplate& qt,
                           char const prefix[],
                           std::shared_ptr<T> t) {
    if (t.get()) {
        qt.append(prefix);
        t->renderTo(qt);
    }
}

template <typename T>
inline void
cloneIf(std::shared_ptr<T>& dest, std::shared_ptr<T> source) {
    if (source != nullptr) dest = source->clone();
}

template <typename T>
inline void
copySyntaxIf(std::shared_ptr<T>& dest, std::shared_ptr<T> source) {
    if (source != nullptr) dest = source->copySyntax();
}

} // namespace


namespace lsst {
namespace qserv {
namespace query {

////////////////////////////////////////////////////////////////////////
// class SelectStmt
////////////////////////////////////////////////////////////////////////

QueryTemplate
SelectStmt::getQueryTemplate() const {
    QueryTemplate qt;
    std::string selectQuant = "SELECT";
    if (_hasDistinct) {
        selectQuant += " DISTINCT";
    }
    qt.setAliasMode(QueryTemplate::DEFINE_VALUE_ALIAS_USE_TABLE_ALIAS);
    renderTemplate(qt, selectQuant.c_str(), _selectList);
    qt.setAliasMode(QueryTemplate::DEFINE_TABLE_ALIAS);
    renderTemplate(qt, "FROM", _fromList);
    qt.setAliasMode(QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS); // column aliases are not allowed in the WHERE clause.
    renderTemplate(qt, "WHERE", _whereClause);
    qt.setAliasMode(QueryTemplate::USE_ALIAS);
    renderTemplate(qt, "GROUP BY", _groupBy);
    renderTemplate(qt, "HAVING", _having);
    renderTemplate(qt, "ORDER BY", _orderBy);

    if (_limit != -1) {
        std::stringstream ss;
        ss << _limit;
        qt.append("LIMIT");
        qt.append(ss.str());
    }
    return qt;
}


std::shared_ptr<WhereClause const>
SelectStmt::getWhere() const {
    return _whereClause;
}


std::shared_ptr<SelectStmt>
SelectStmt::clone() const {
    std::shared_ptr<SelectStmt> newS = std::make_shared<SelectStmt>(*this);
    // Starting from a shallow copy, make a copy of the syntax portion.
    cloneIf(newS->_fromList, _fromList);
    cloneIf(newS->_selectList, _selectList);
    cloneIf(newS->_whereClause, _whereClause);
    cloneIf(newS->_orderBy, _orderBy);
    cloneIf(newS->_groupBy, _groupBy);
    cloneIf(newS->_having, _having);
    assert(_hasDistinct == newS->_hasDistinct);
    // For the other fields, default-copied versions are okay.
    return newS;
}


void SelectStmt::setFromListAsTable(std::string const& t) {
    TableRefListPtr tr = std::make_shared<TableRefList>();
    tr->push_back(std::make_shared<TableRef>("", t, ""));
    _fromList = std::make_shared<FromList>(tr);
}


bool SelectStmt::operator==(const SelectStmt& rhs) const {
    return (util::ptrCompare<FromList>(_fromList, rhs._fromList) &&
            util::ptrCompare<SelectList>(_selectList, rhs._selectList) &&
            util::ptrCompare<WhereClause>(_whereClause, rhs._whereClause) &&
            util::ptrCompare<OrderByClause>(_orderBy, rhs._orderBy) &&
            util::ptrCompare<GroupByClause>(_groupBy, rhs._groupBy) &&
            util::ptrCompare<HavingClause>(_having, rhs._having) &&
            _hasDistinct == rhs._hasDistinct &&
            _limit == rhs._limit &&
            OutputMods == rhs.OutputMods);
}


std::ostream& operator<<(std::ostream& os, SelectStmt const& selectStmt) {
    os << "SelectStmt(";
    os << selectStmt._selectList;
    os << ", " << selectStmt._fromList;
    os << ", " << selectStmt._whereClause;
    os << ", " << selectStmt._orderBy;
    os << ", " << selectStmt._groupBy;
    os << ", " << selectStmt._having;
    os << ", " << selectStmt._hasDistinct;
    os << ", " << selectStmt._limit;
    if (selectStmt.OutputMods.empty() == false) {
        os << ", " << util::printable(selectStmt.OutputMods, "", "");
    }
    os << ")";
    return os;
}


}}} // namespace lsst::qserv::query
