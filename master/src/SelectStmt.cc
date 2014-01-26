/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
  * @file SelectStmt.cc
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

#include "lsst/qserv/master/SelectStmt.h"

// Standard
#include <map>

// Boost
#include <boost/algorithm/string/predicate.hpp> // string iequal

// Local (placed in src/)
#include "SqlSQL2Parser.hpp"
#include "SqlSQL2TokenTypes.hpp"

#include "lsst/qserv/Logger.h"

#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/ColumnRefH.h"
#include "lsst/qserv/master/SelectList.h"
#include "lsst/qserv/master/FromList.h"
#include "lsst/qserv/master/GroupByClause.h"
#include "lsst/qserv/master/OrderByClause.h"
#include "lsst/qserv/master/HavingClause.h"
#include "lsst/qserv/master/WhereClause.h"
#include "lsst/qserv/master/WhereFactory.h"
// myself


// namespace modifiers
namespace qMaster = lsst::qserv::master;


////////////////////////////////////////////////////////////////////////
// Experimental
////////////////////////////////////////////////////////////////////////

// forward

////////////////////////////////////////////////////////////////////////
// anonymous
////////////////////////////////////////////////////////////////////////
namespace {
template <typename T>
inline void renderTemplate(qMaster::QueryTemplate& qt,
                           char const prefix[],
                           boost::shared_ptr<T> t) {
    if(t.get()) {
        qt.append(prefix);
        t->renderTo(qt);
    }
}
template <typename T>
inline void
copyDeepIf(boost::shared_ptr<T>& dest, boost::shared_ptr<T> source) {
    if(source.get()) dest = source->copyDeep();
}
template <typename T>
inline void
copySyntaxIf(boost::shared_ptr<T>& dest, boost::shared_ptr<T> source) {
    if(source.get()) dest = source->copySyntax();
}
} // namespace
////////////////////////////////////////////////////////////////////////
// class SelectStmt
////////////////////////////////////////////////////////////////////////

// Hook into parser to get populated.
qMaster::SelectStmt::SelectStmt() {
}

void qMaster::SelectStmt::diagnose() {
    //_selectList->getColumnRefList()->printRefs();
    _selectList->dbgPrint();
    _generate();

}

qMaster::QueryTemplate
qMaster::SelectStmt::getTemplate() const {
    QueryTemplate qt;
    renderTemplate(qt, "SELECT", _selectList);
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
qMaster::QueryTemplate
qMaster::SelectStmt::getPostTemplate() const {
    QueryTemplate qt;
    renderTemplate(qt, "GROUP BY", _groupBy);
    renderTemplate(qt, "HAVING", _having);
    renderTemplate(qt, "ORDER BY", _orderBy);
    return qt;
}

boost::shared_ptr<qMaster::WhereClause const>
qMaster::SelectStmt::getWhere() const {
    return _whereClause;
}

boost::shared_ptr<qMaster::SelectStmt>
qMaster::SelectStmt::copyDeep() const {
    boost::shared_ptr<SelectStmt> newS(new SelectStmt(*this));
    // Starting from a shallow copy, make a copy of the syntax portion.
    copyDeepIf(newS->_fromList, _fromList);
    copyDeepIf(newS->_selectList, _selectList);
    copyDeepIf(newS->_whereClause, _whereClause);
    copyDeepIf(newS->_orderBy, _orderBy);
    copyDeepIf(newS->_groupBy, _groupBy);
    copyDeepIf(newS->_having, _having);
    // For the other fields, default-copied versions are okay.
    return newS;
}

boost::shared_ptr<qMaster::SelectStmt>
qMaster::SelectStmt::copyMerge() const {
    boost::shared_ptr<SelectStmt> newS(new SelectStmt(*this));
    // Starting from a shallow copy, copy only the pieces that matter
    // for the merge clause.
    copySyntaxIf(newS->_selectList, _selectList);
    copySyntaxIf(newS->_orderBy, _orderBy);
    copySyntaxIf(newS->_groupBy, _groupBy);
    copySyntaxIf(newS->_having, _having);
    // Eliminate the parts that don't matter, e.g., the where clause
    newS->_whereClause.reset();
    newS->_fromList.reset();
    return newS;
}

boost::shared_ptr<qMaster::SelectStmt>
qMaster::SelectStmt::copySyntax() const {
    boost::shared_ptr<SelectStmt> newS(new SelectStmt(*this));
    // Starting from a shallow copy, make a copy of the syntax portion.
    copySyntaxIf(newS->_fromList, _fromList);
    copySyntaxIf(newS->_selectList, _selectList);
    copySyntaxIf(newS->_whereClause, _whereClause);
    copySyntaxIf(newS->_orderBy, _orderBy);
    copySyntaxIf(newS->_groupBy, _groupBy);
    copySyntaxIf(newS->_having, _having);
    // For the other fields, default-copied versions are okay.
    return newS;
}

void
qMaster::SelectStmt::hasOrderBy(bool shouldHave) {
    if(!shouldHave) { _orderBy.reset(); } // Get rid of it
    // Make it if we want one.
    else if(!_orderBy) { _orderBy.reset(new OrderByClause()); }
}

////////////////////////////////////////////////////////////////////////
// class SelectStmt (private)
////////////////////////////////////////////////////////////////////////
namespace {
template <typename OS, typename T>
inline OS& print(OS& os, char const label[], boost::shared_ptr<T> t) {
    if(t.get()) {
        os << label << ": " << *t << std::endl;
    }
    return os;
}
template <typename OS, typename T>
inline OS& generate(OS& os, char const label[], boost::shared_ptr<T> t) {
    if(t.get()) {
        os << label << " " << t->getGenerated() << std::endl;
    }
    return os;
}
} // anonymous

void qMaster::SelectStmt::_print() {
    //_selectList->getColumnRefList()->printRefs();
    print(LOG_STRM(Info), "from", _fromList);
    print(LOG_STRM(Info), "select", _selectList);
    print(LOG_STRM(Info), "where", _whereClause);
    print(LOG_STRM(Info), "groupby", _groupBy);
    print(LOG_STRM(Info), "having", _having);
    print(LOG_STRM(Info), "orderby", _orderBy);
    if(_limit != -1) { LOGGER_INF << " LIMIT " << _limit; }
}

void qMaster::SelectStmt::_generate() {
    LOGGER_INF << getTemplate().dbgStr() << std::endl;
}
