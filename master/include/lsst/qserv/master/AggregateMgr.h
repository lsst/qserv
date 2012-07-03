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

// AggregateMgr.h: 
// class AggregateMgr -- top-level class
// class AggregateMgr::AliasVal -- value class for aliases
//
// -- Specific classes for constructing AggregateRecords --
// class AggregateMgr::{AggBuilderIf, 
//                      EasyAggBuilder, CountAggBuilder, AvgAggBuilder}
//
// -- Handlers that get plugged into the parser.
// class AggregateMgr::{SetFuncHandler, AliasHandler, SelectListHandler, 
//                      GroupByHandler, GroupColumnHandler}
//
// class AggregateRecord
// NodeBound, NodeList typedefs
//
// AggregateMgr records aggregation needs detected in a top-level
// query and generates appropriate clauses for use in chunk queries
// and merge queries. 
 
#ifndef LSST_QSERV_MASTER_AGGREGATEMGR_H
#define LSST_QSERV_MASTER_AGGREGATEMGR_H

// C++ 
#include <deque>
#include <map>

// Boost
#include <boost/shared_ptr.hpp>

// ANTLR
//#include "antlr/AST.hpp"
//#include "antlr/CommonAST.hpp"
#include "antlr/ASTRefCount.hpp"

// Package:
#include "lsst/qserv/master/parserBase.h" 
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/parseHandlers.h"
#include "lsst/qserv/master/Callback.h"

namespace lsst {
namespace qserv {
namespace master {

// Aggregate Record is a value class for all the information you need
// to successfully perform aggregation of distributed queries.
// lbl and meaning record the original aggregation invocation (+alias)
// orig, pass, and fixup record SQL expressions
class AggregateRecord {
public:
    NodeBound lbl;
    NodeBound meaning; 
    std::string orig; // Original SQL expression
    std::string pass; // SQL expression passed in subquery
    std::string fixup; // SQL expression used during merging/fixup
    std::ostream& printTo(std::ostream& os);
    void fillStandard(NodeBound const& lbl_, NodeBound const& meaning_);
    std::string getFuncParam() const;
    std::string getLabelText() const;
};

// AggregateMgr glues together the functionality needed to detect 
// aggregation in a query and figure out the right things to do
// in subqueries and results preparation.
class AggregateMgr {
public:
    typedef std::map<antlr::RefAST, AggregateRecord> AggMap;
    typedef std::deque<Callback::Ptr> CallbackDeque;
    class AliasVal;
    class AggBuilderIf;
    class EasyAggBuilder;
    class CountAggBuilder;
    class AvgAggBuilder;
    
    class SetFuncHandler;
    class AliasHandler;
    class SelectListHandler;
    class GroupByHandler;
    class GroupColumnHandler;

    AggregateMgr(AliasMgr& am);
    
    void postprocess(AliasMgr::NodeMap const& aMap);
    void applyAggPass();
    void listenSelectReceived(Callback::Ptr c);
    void signalSelectReceived();

    std::string getPassSelect();
    std::string getFixupSelect();
    std::string getFixupPost();
    
    bool getHasAggregate() const { return _hasAggregate; }
    boost::shared_ptr<VoidOneRefFunc> getSetFuncHandler();
    boost::shared_ptr<VoidOneRefFunc> getSelectListHandler();
    boost::shared_ptr<VoidOneRefFunc> newSelectStarHandler();
    boost::shared_ptr<VoidOneRefFunc> getGroupByHandler();
    boost::shared_ptr<VoidOneRefFunc> getGroupColumnHandler();

private:
    void _computeSelects();
    void _computePost();
    boost::shared_ptr<SetFuncHandler> _setFuncer;
    boost::shared_ptr<SelectListHandler> _selectLister;
    boost::shared_ptr<GroupByHandler> _groupByer;
    boost::shared_ptr<GroupColumnHandler> _groupColumner;
    AggMap _aggRecords;
    std::string _passSelect;
    std::string _fixupSelect;
    std::string _fixupPost;
    bool _hasAggregate;
    bool _isMissingSelect;
    CallbackDeque _selectCallbacks;

}; // class AggregateMgr

// AliasVal records an alias definition in an ANTLR AST
class AggregateMgr::AliasVal {
public:
    AliasVal(antlr::RefAST lbl_, antlr::RefAST meaning_) : lbl(lbl_), meaning(meaning_){}
    antlr::RefAST lbl;
    antlr::RefAST meaning;
};

// AggBuilderIf is an interface supported by objects that can construct
// AggregateRecords from alias values.
class AggregateMgr::AggBuilderIf {
public:
    typedef boost::shared_ptr<AggBuilderIf> Ptr;
    virtual AggregateRecord operator()(NodeBound const& lbl,
                                       NodeBound const& meaning) = 0;
    virtual ~AggBuilderIf() {}
};

// EasyAggBuilder builds AggregateRecords that are easy--same
// expression is passed into subqueries, and is used during merging
// and result preparation. 
class AggregateMgr::EasyAggBuilder : public AggregateMgr::AggBuilderIf {
public:
    virtual AggregateRecord operator()(NodeBound const& lbl,
                                       NodeBound const& meaning);
private:
    std::string _computeFixup(AggregateRecord const& a); 
};

// CountAggBuilder builds AggregateRecords for COUNT() aggregations.
class AggregateMgr::CountAggBuilder : public AggregateMgr::AggBuilderIf {
public:
    virtual AggregateRecord operator()(NodeBound const& lbl,
                                       NodeBound const& meaning);
private:
    std::string _computeFixup(AggregateRecord const& a); 
};

// CountAggBuilder builds AggregateRecords for AVG() aggregations.
class AggregateMgr::AvgAggBuilder : public AggregateMgr::AggBuilderIf {
public:
    virtual AggregateRecord operator()(NodeBound const& lbl,
                                       NodeBound const& meaning);
private:
    void _computePassFixup(AggregateRecord& a); 
};
    
// SetFuncHandler is bolted to the SQL parser, where it is called when
// the parser detects a function call.
class AggregateMgr::SetFuncHandler : public VoidOneRefFunc {
public: 
    typedef std::map<std::string, AggBuilderIf::Ptr> Map;
    typedef Map::const_iterator MapConstIter;
    typedef Map::iterator MapIter;

    typedef std::deque<NodeBound> Deque;
    typedef Deque::const_iterator DequeConstIter;
    typedef Deque::iterator DequeIterator;
	
    SetFuncHandler();
    virtual ~SetFuncHandler() {}
    virtual void operator()(antlr::RefAST a);
    Deque const& getAggs() const { return _aggs; }
    Map& getProcs() { return _map; }
private:
    Deque _aggs;
    Map _map;
}; // class AggregateMgr::SetFuncHandler


//  SelectListHandler is bolted to the parser so it gets called once
//  the column/reference list is detected.
class AggregateMgr::SelectListHandler : public VoidOneRefFunc {
public: 
    class SelectStarHandler : public VoidOneRefFunc {
    public: 
        explicit SelectStarHandler(SelectListHandler& h) : handler(h) {}
        virtual ~SelectStarHandler() {}
        virtual void operator()(antlr::RefAST a) { handler.handleSelectStar(); }
        SelectListHandler& handler;
    };

    typedef std::deque<NodeList> Deque;
    SelectListHandler(AliasMgr& am, AggregateMgr& agm);
    virtual ~SelectListHandler() {}
    virtual void operator()(antlr::RefAST a);
    void handleSelectStar() { 
        if(selectLists.empty()) { 
            isStarFirst = true; 
        } 
        _aggMgr.signalSelectReceived();
    }
    boost::shared_ptr<SelectStarHandler> newSelectStarHandler() {
        typedef boost::shared_ptr<SelectStarHandler> Ptr;
        return Ptr(new SelectStarHandler(*this));
    }
    AliasMgr& _aMgr; // Get help from AliasHandler
    AggregateMgr& _aggMgr; // For signaling AggregateMgr
    Deque selectLists;
    NodeBound firstSelectBound;
    bool isStarFirst;
}; // class SelectListHandler

// GroupByHandler is called when a GROUP BY clause is detected
class AggregateMgr::GroupByHandler : public VoidOneRefFunc {
public: 
    GroupByHandler() : _isFrozen(false) {}
    virtual ~GroupByHandler() {}
    virtual void operator()(antlr::RefAST a);
    void addColumn(NodeBound const& n);
    std::string getGroupByString() const;
    bool getHasColumns() { return !_columns.empty(); }
private:
    NodeList _columns;
    bool _isFrozen;
}; // class GroupByHandler

// GroupColumnHandler is called once per column referenced in a GROUP
// BY predicate.
class AggregateMgr::GroupColumnHandler : public VoidOneRefFunc {
public: 
    GroupColumnHandler(GroupByHandler& h_) :h(h_) {}
    virtual ~GroupColumnHandler() {}
    virtual void operator()(antlr::RefAST a);
    GroupByHandler& h;
}; // class GroupColumnHandler

// AggregateMgr inlines
inline 
boost::shared_ptr<VoidOneRefFunc> AggregateMgr::getSetFuncHandler() {
    return _setFuncer;
}

inline
boost::shared_ptr<VoidOneRefFunc> AggregateMgr::getSelectListHandler() {
    return _selectLister;
}

inline
boost::shared_ptr<VoidOneRefFunc> AggregateMgr::newSelectStarHandler() {
    return _selectLister->newSelectStarHandler();
}

inline
boost::shared_ptr<VoidOneRefFunc> AggregateMgr::getGroupByHandler() {
    return _groupByer; 
}

inline
boost::shared_ptr<VoidOneRefFunc> AggregateMgr::getGroupColumnHandler() { 
    return _groupColumner;
}
////////////////////////////////////////////////////////////////////////


}}} // lsst::qserv::master
#endif // LSST_QSERV_MASTER_AGGREGATEMGR_H
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             
