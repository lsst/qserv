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

namespace lsst {
namespace qserv {
namespace master {

typedef std::pair<antlr::RefAST, antlr::RefAST> NodeBound;
typedef std::deque<NodeBound> NodeList;
typedef NodeList::const_iterator NodeListConstIter;

// Aggregate Record is a value class for all the information you need
// to successfully perform distributed aggregation.
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

    AggregateMgr();
    
    void postprocess();
    void applyAggPass();

    std::string getPassSelect();
    std::string getFixupSelect();
    std::string getFixupPost();
    
    bool getHasAggregate() const { return _hasAggregate; }
    boost::shared_ptr<VoidTwoRefFunc> getAliasHandler(); 
    boost::shared_ptr<VoidOneRefFunc> getSetFuncHandler();
    boost::shared_ptr<VoidOneRefFunc> getSelectListHandler();
    boost::shared_ptr<VoidVoidFunc> newSelectStarHandler();
    boost::shared_ptr<VoidOneRefFunc> getGroupByHandler();
    boost::shared_ptr<VoidOneRefFunc> getGroupColumnHandler();

private:
    void _computeSelects();
    void _computePost();
    boost::shared_ptr<AliasHandler> _aliaser;
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

}; // class AggregateMgr

// AliasVal records an alias definition
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

// AliasHandler is bolted to the SQL parser, where it gets called for
// each aliasing instance.
class AggregateMgr::AliasHandler : public VoidTwoRefFunc {
public: 
    typedef std::map<antlr::RefAST, NodeBound> Map;
    typedef Map::const_iterator MapConstIter;
    typedef Map::iterator MapIter;

    AliasHandler() {}
    virtual ~AliasHandler() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
        using lsst::qserv::master::getLastSibling;
        if(b.get()) {
            _map[a] = NodeBound(b, getLastSibling(a));
        }
        _nodes.push_back(NodeBound(a, getLastSibling(a)));
        // Save column ref for pass/fixup computation, 
        // regardless of alias.
    }
    Map const& getInvAliases() const { return _map; }
    NodeList getNodeListCopy() { return _nodes; }
    void resetNodeList() { _nodes.clear(); }
private:
    Map _map;
    NodeList _nodes;
}; // class AggregateMgr::AliasHandler

//  SelectListHandler is bolted to the parser so it gets called once
//  the column/reference list is detected.
class AggregateMgr::SelectListHandler : public VoidOneRefFunc {
public: 
    class SelectStarHandler : public VoidVoidFunc {
    public: 
        SelectStarHandler(SelectListHandler& h) : handler(h) {}
        virtual ~SelectStarHandler() {}
        virtual void operator()() { handler.handleSelectStar(); }
        SelectListHandler& handler;
    };
    // typedef std::deque<antlr::RefAST> SelectList;
    // typedef SelectList::const_iterator SelectListConstIter;
    // typedef std::deque<SelectList> Deque;
    typedef std::deque<NodeList> Deque;
    SelectListHandler(AliasHandler& h);
    virtual ~SelectListHandler() {}
    virtual void operator()(antlr::RefAST a);
    void handleSelectStar() { 
        if(selectLists.empty()) { 
            isStarFirst = true; 
        } 
    }
    boost::shared_ptr<SelectStarHandler> newSelectStarHandler() {
        typedef boost::shared_ptr<SelectStarHandler> Ptr;
        return Ptr(new SelectStarHandler(*this));
    }
    AliasHandler& _aHandler; // Get help from AliasHandler
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
boost::shared_ptr<VoidTwoRefFunc> AggregateMgr::getAliasHandler() {
    return _aliaser;
}

inline 
boost::shared_ptr<VoidOneRefFunc> AggregateMgr::getSetFuncHandler() {
    return _setFuncer;
}

inline
boost::shared_ptr<VoidOneRefFunc> AggregateMgr::getSelectListHandler() {
    return _selectLister;
}

inline
boost::shared_ptr<VoidVoidFunc> AggregateMgr::newSelectStarHandler() {
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
