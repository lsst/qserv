// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
// WhereClause.h is a representation of a parsed SQL WHERE.

#ifndef LSST_QSERV_MASTER_WHERECLAUSE_H
#define LSST_QSERV_MASTER_WHERECLAUSE_H


// Std
#include <iostream>
#include <list>
#include <stack>
// Boost
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
// Qserv
#include "lsst/qserv/master/ColumnRefList.h"
#include "lsst/qserv/master/BoolTerm.h"

#if 0
#include <map>
#include <deque>
#include <antlr/AST.hpp>


#include "lsst/qserv/master/TableRefN.h"
#endif

namespace lsst { namespace qserv { namespace master {
class BoolTerm; // Forward

class QsRestrictor {
public:
    typedef boost::shared_ptr<QsRestrictor> Ptr;
    typedef std::list<Ptr> List;
    typedef std::list<std::string> StringList;

    class render {
    public:
        render(QueryTemplate& qt_) : qt(qt_) {}
        void operator()(QsRestrictor::Ptr const& p);
        QueryTemplate& qt;
    };

    std::string _name;
    StringList _params;
};

class WhereClause {
public:
    WhereClause() : _columnRefList(new ColumnRefList()) {}
    ~WhereClause() {}
    class ValueExprIter; // iteratable interface.
    friend class ValueExprIter; 

    boost::shared_ptr<ColumnRefList> getColumnRefList() { 
        return _columnRefList; }
    boost::shared_ptr<QsRestrictor::List const> getRestrs() const {
        return _restrs; }
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
    friend class WhereFactory;
    
    std::string _original;
    boost::shared_ptr<ColumnRefList> _columnRefList;
    boost::shared_ptr<BoolTerm> _tree;
    boost::shared_ptr<QsRestrictor::List> _restrs;

};

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

    ValueExprTerm* _checkForExpr();
    ValueExprTerm* _checkForExpr() const;
    void _incrementBfTerm();
    void _incrementBterm();
    bool _findFactor();
    bool _setupBfIter();
    

    WhereClause* _wc; // no shared_ptr available
    // A position tuple is: cursor, end
    typedef std::pair<BoolTerm::PtrList::iterator,
                      BoolTerm::PtrList::iterator> PosTuple;
    std::stack<PosTuple> _posStack;
    BfTerm::PtrList::iterator _bfIter;
    BfTerm::PtrList::iterator _bfEnd;
};


}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_WHERECLAUSE_H

