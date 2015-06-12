// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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
#include "query/WhereClause.h"


////////////////////////////////////////////////////////////////////////
// anonymous
////////////////////////////////////////////////////////////////////////
namespace {
template <typename T>
inline void renderTemplate(lsst::qserv::query::QueryTemplate& qt,
                           char const prefix[],
                           std::shared_ptr<T> t) {
    if(t.get()) {
        qt.append(prefix);
        t->renderTo(qt);
    }
}
template <typename T>
inline void
cloneIf(std::shared_ptr<T>& dest, std::shared_ptr<T> source) {
    if(source.get()) dest = source->clone();
}
template <typename T>
inline void
copySyntaxIf(std::shared_ptr<T>& dest, std::shared_ptr<T> source) {
    if(source.get()) dest = source->copySyntax();
}
} // namespace


namespace lsst {
namespace qserv {
namespace query {

////////////////////////////////////////////////////////////////////////
// class SelectStmt
////////////////////////////////////////////////////////////////////////

SelectStmt::SelectStmt() {
}

QueryTemplate
SelectStmt::getQueryTemplate() const {
    QueryTemplate qt;
    std::string selectQuant = "SELECT";
    if(_hasDistinct) {
        selectQuant += " DISTINCT";
    }
    renderTemplate(qt, selectQuant.c_str(), _selectList);
    renderTemplate(qt, "FROM", _fromList);
    renderTemplate(qt, "WHERE", _whereClause);
    renderTemplate(qt, "GROUP BY", _groupBy);
    renderTemplate(qt, "HAVING", _having);
    renderTemplate(qt, "ORDER BY", _orderBy);

    if(_limit != -1) {
        std::stringstream ss;
        ss << _limit;
        qt.append("LIMIT");
        qt.append(ss.str());
    }
    return qt;
}

/// getPostTemplate() is specialized to the needs of generating a
/// "post" string for the aggregating table merger MergeFixup
/// object. Hopefully, we will port the merger to use the merging
/// statement more as-is (just patching the FROM part).
QueryTemplate
SelectStmt::getPostTemplate() const {
    QueryTemplate qt;
    renderTemplate(qt, "GROUP BY", _groupBy);
    renderTemplate(qt, "HAVING", _having);
    renderTemplate(qt, "ORDER BY", _orderBy);
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

std::shared_ptr<SelectStmt>
SelectStmt::copyMerge() const {
    std::shared_ptr<SelectStmt> newS = std::make_shared<SelectStmt>(*this);
    // Starting from a shallow copy, copy only the pieces that matter
    // for the merge clause.
    copySyntaxIf(newS->_selectList, _selectList);
    copySyntaxIf(newS->_orderBy, _orderBy);
    copySyntaxIf(newS->_groupBy, _groupBy);
    copySyntaxIf(newS->_having, _having);
    // Eliminate the parts that don't matter, e.g., the where clause
    newS->_whereClause.reset();
    newS->_fromList.reset();
    assert(_hasDistinct == newS->_hasDistinct);
    return newS;
}

std::shared_ptr<SelectStmt>
SelectStmt::copySyntax() const {
    std::shared_ptr<SelectStmt> newS = std::make_shared<SelectStmt>(*this);
    // Starting from a shallow copy, make a copy of the syntax portion.
    copySyntaxIf(newS->_fromList, _fromList);
    copySyntaxIf(newS->_selectList, _selectList);
    copySyntaxIf(newS->_whereClause, _whereClause);
    copySyntaxIf(newS->_orderBy, _orderBy);
    copySyntaxIf(newS->_groupBy, _groupBy);
    copySyntaxIf(newS->_having, _having);
    assert(_hasDistinct == newS->_hasDistinct);
    // For the other fields, default-copied versions are okay.
    return newS;
}

void SelectStmt::setFromListAsTable(std::string const& t) {
    TableRefListPtr tr = std::make_shared<TableRefList>();
    tr->push_back(std::make_shared<TableRef>("", t, ""));
    _fromList = std::make_shared<FromList>(tr);
}

////////////////////////////////////////////////////////////////////////
// class SelectStmt (private)
////////////////////////////////////////////////////////////////////////
namespace {

template <typename OS, typename T>
inline OS& print(OS& os, char const label[], std::shared_ptr<T> t) {
    if(t.get()) {
        os << label << ": " << *t << std::endl;
    }
    return os;
}
template <typename OS, typename T>
inline OS& generate(OS& os, char const label[], std::shared_ptr<T> t) {
    if(t.get()) {
        os << label << " " << t->getGenerated() << std::endl;
    }
    return os;
}

template<class T>
void nil_string_helper(std::ostringstream& oss,
		       const std::shared_ptr<T> &ptr) {
	if (ptr) oss << *ptr << " ";
}

} // anonymous


std::string SelectStmt::toString() {

    //_selectList->getColumnRefList()->printRefs();
    std::ostringstream oss;

    nil_string_helper(oss, _selectList);
    nil_string_helper(oss, _fromList);
    if(_hasDistinct) {
        oss << "DISTINCT ";
    }
    nil_string_helper(oss, _whereClause);
    nil_string_helper(oss, _groupBy);
    nil_string_helper(oss, _having);
    nil_string_helper(oss, _orderBy);
    if(_limit != -1) {
        oss << " LIMIT " << _limit;
    }
    return oss.str();
}

std::string SelectStmt::toQueryTemplateString() {
    //_selectList->getColumnRefList()->printRefs();
    //_selectList->dbgPrint(std::cout);
    return getQueryTemplate().toString();
}

}}} // namespace lsst::qserv::query
