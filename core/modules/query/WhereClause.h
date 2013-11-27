// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_QUERY_WHERECLAUSE_H
#define LSST_QSERV_QUERY_WHERECLAUSE_H
/**
  * @file WhereClause.h
  *
  * @brief WhereClause is a parsed SQL WHERE; QsRestrictor is a queryspec
  * spatial restrictor.
  *
  * @author Daniel L. Wang, SLAC
  */

// Standard
#include <iostream>
#include <list>
#include <stack>
// Boost
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/iterator_adaptors.hpp>
// Qserv
#include "query/ColumnRef.h"
#include "query/BoolTerm.h"
#include "query/QsRestrictor.h"
#include "query/ValueExpr.h"

namespace lsst {
namespace qserv {

namespace parser {
    // Forward
    class WhereFactory;
}   

namespace query {

class BoolTerm; // Forward

/// WhereClause is a SQL WHERE containing QsRestrictors and a BoolTerm tree.
class WhereClause {
public:
    WhereClause() {}
    ~WhereClause() {}
    class ValueExprIter; // iteratable interface.
    friend class ValueExprIter;

    boost::shared_ptr<QsRestrictor::List const> getRestrs() const {
        return _restrs; }
    boost::shared_ptr<BoolTerm const> getRootTerm() const {
        return _tree;
    }
    boost::shared_ptr<ColumnRef::List const> getColumnRefs() const;
    boost::shared_ptr<AndTerm> getRootAndTerm();

    ValueExprIter vBegin();
    ValueExprIter vEnd();

    std::string getGenerated() const;
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<WhereClause> copyDeep() const;
    boost::shared_ptr<WhereClause> copySyntax();

    void resetRestrs();
    void prependAndTerm(boost::shared_ptr<BoolTerm> t);

private:
    friend std::ostream& operator<<(std::ostream& os, WhereClause const& wc);
    friend class parser::WhereFactory;

    std::string _original;
    boost::shared_ptr<BoolTerm> _tree;
    boost::shared_ptr<QsRestrictor::List> _restrs;

};

/// ValueExprIter facilitates iteration over value expressions in WhereClause
/// objects for analysis and manipulation.
class WhereClause::ValueExprIter : public boost::iterator_facade <
    WhereClause::ValueExprIter, ValueExprPtr, boost::forward_traversal_tag> {
public:
    ValueExprIter() : _wc() {}

private:
    explicit ValueExprIter(WhereClause* wc,
                           boost::shared_ptr<BoolTerm> bPos);

    friend class WhereClause;
    friend class boost::iterator_core_access;

    void increment();
    bool equal(ValueExprIter const& other) const;
    ValueExprPtr& dereference() const;
    ValueExprPtr& dereference();

    bool _checkIfValid() const;
    void _incrementValueExpr();
    void _incrementBfTerm();
    void _incrementBterm();
    bool _findFactor();
    void _reset();
    bool _setupBfIter();
    void _updateValueExprIter();


    WhereClause* _wc; // no shared_ptr available
    // A position tuple is: cursor, end
    typedef std::pair<BoolTerm::PtrList::iterator,
                      BoolTerm::PtrList::iterator> PosTuple;
    std::stack<PosTuple> _posStack;
    BfTerm::PtrList::iterator _bfIter;
    BfTerm::PtrList::iterator _bfEnd;
    typedef ValueExprList::iterator ValueExprListIter;
    ValueExprListIter _vIter;
    ValueExprListIter _vEnd;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_WHERECLAUSE_H
