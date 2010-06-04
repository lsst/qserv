#include <sstream>
#include <boost/format.hpp>
#include "lsst/qserv/master/AggregateMgr.h"
using lsst::qserv::master::AggregateMgr;
using lsst::qserv::master::AggregateRecord;
using lsst::qserv::master::NodeBound;



////////////////////////////////////////////////////////////////////////
// AggregateRecord
////////////////////////////////////////////////////////////////////////
std::ostream& AggregateRecord::printTo(std::ostream& os) {
    os << "Aggregate orig=" << orig << std::endl 
       << "pass=" << pass  << std::endl
       << "fixup=" << fixup;
}
void AggregateRecord::fillStandard(NodeBound const& lbl_, 
				   NodeBound const& meaning_) {
    
    // Fill label and meaning 
    // (i.e., label=bmagSum, meaning=sum(bmag) for sum(bmag) as bmagSum)
    lbl = lbl_;
    meaning = meaning_;
    if(lbl.first != meaning.first) { // If there is an alias, include it.
	assert(lbl.second.get()); // must have bound.
		orig = walkBoundedTreeString(meaning.first, lbl.second);
    } else { // No alias. Use meaning only
	orig = walkBoundedTreeString(meaning.first, meaning.second);
    }
}
std::string AggregateRecord::getFuncParam() const {
    antlr::RefAST lParen = meaning.first->getNextSibling();
    assert(lParen.get());
    antlr::RefAST paramAst = lParen->getNextSibling();
    assert(paramAst.get());
    std::string p = getFuncString(paramAst);
    if(')' == *(p.rbegin())) { // chop the end, if it's a ')'
	p.erase(p.size()-1);
    }
    return p;
}

std::string AggregateRecord::getLabelText() const {
    return walkBoundedTreeString(lbl.first, lbl.second);
}

////////////////////////////////////////////////////////////////////////
// AggregateMgr::EasyAggBuilder
////////////////////////////////////////////////////////////////////////
AggregateRecord AggregateMgr::EasyAggBuilder::operator()(NodeBound const& lbl,
							 NodeBound const& meaning) {
    AggregateRecord a;
    a.fillStandard(lbl, meaning);
    a.pass = a.orig;
    a.fixup = _computeFixup(a);
    return a;
}


std::string AggregateMgr::EasyAggBuilder::_computeFixup(AggregateRecord const& a) {
    std::string agg = tokenText(a.meaning.first);
    std::string lblText = a.getLabelText();
    // Orig: agg ( param ) lbl
    // Fixup: agg ( quoted-lbl) 
    return agg + "(`" + lblText + "`) AS `" + lblText + "`";
}
////////////////////////////////////////////////////////////////////////
// AggregateMgr::CountAggBuilder
////////////////////////////////////////////////////////////////////////
AggregateRecord AggregateMgr::CountAggBuilder::operator()(NodeBound const& lbl,
							  NodeBound const& meaning) {
    AggregateRecord a;
    a.fillStandard(lbl, meaning);
    a.pass = a.orig;
    a.fixup = _computeFixup(a);
    return a;
}

std::string AggregateMgr::CountAggBuilder::_computeFixup(AggregateRecord const& a) {
    std::string agg = "SUM"; //tokenText(meaning.first);
    std::string lblText = a.getLabelText();
    // Orig: agg ( param ) lbl
    // Fixup: agg ( quoted-lbl) 
    return agg + "(`" + lblText + "`) AS `" + lblText + "`";
}

////////////////////////////////////////////////////////////////////////
// AggregateMgr::AvgAggBuilder
////////////////////////////////////////////////////////////////////////
AggregateRecord AggregateMgr::AvgAggBuilder::operator()(NodeBound const& lbl,
							NodeBound const& meaning) {
    AggregateRecord a;
    a.fillStandard(lbl, meaning);
    _computePassFixup(a);
    return a;
}


void AggregateMgr::AvgAggBuilder::_computePassFixup(AggregateRecord& a) { 
    std::string param = a.getFuncParam();
    // Consider sanitizing param to eliminate problematic symbols

    // Convert avg(x) to sum(x) as avgs_x,count(x) as avgc_x for pass.;
    std::string sumAlias = "avgs_" + param;
    std::string countAlias = "avgc_" + param;
    boost::format passFmt("SUM(%1%) AS %2%, COUNT(%1%) AS %3%");
    a.pass = (passFmt % param % sumAlias % countAlias).str();
    // Convert avg(x) to sum(avgs_x)/sum(avgc_x) `avg(epoch)` for fixup.
    boost::format fixFmt("SUM(%1%)/SUM(%2%) AS `%3%`");
    a.fixup = (fixFmt % sumAlias % countAlias % a.getLabelText()).str();

}


////////////////////////////////////////////////////////////////////////
// AggregateMgr::SetFuncHandler
////////////////////////////////////////////////////////////////////////
AggregateMgr::SetFuncHandler::SetFuncHandler() {
    _map["count"].reset(new CountAggBuilder());
    _map["avg"].reset(new AvgAggBuilder());
    _map["max"].reset(new EasyAggBuilder());
    _map["min"].reset(new EasyAggBuilder());
    _map["sum"].reset(new EasyAggBuilder());
}

void AggregateMgr::SetFuncHandler::operator()(antlr::RefAST a) {
    //std::cout << "---- SETFUNC: ----" << walkTreeString(a) << std::endl;
    std::string origAgg = tokenText(a);
    MapConstIter i = _map.find(origAgg); // case-sensitivity?
    assert(i != _map.end());
    _aggs.push_back(NodeBound(a, getLastSibling(a)));
}
////////////////////////////////////////////////////////////////////////
// AggregateMgr::SelectListHandler
////////////////////////////////////////////////////////////////////////
AggregateMgr::SelectListHandler::SelectListHandler(AliasHandler& h) 
    : _aHandler(h), isStarFirst(false) {
} 

void AggregateMgr::SelectListHandler::operator()(antlr::RefAST a)  {
    using lsst::qserv::master::getLastSibling;
    if(selectLists.size() == 0) {
	firstSelectBound.first = a;
	firstSelectBound.second = getLastSibling(a);
    }
#if 0
    selectLists.push_back(SelectList());
    SelectList& sl = selectLists.back();
    //std::cout << "sl ";
    for(antlr::RefAST i = a; i.get(); 
	i = i->getNextSibling()) { 
	sl.push_back(i);
	//std::cout << tokenText(i) << " ";
    }
    //std::cout << std::endl;
#else
    selectLists.push_back(_aHandler.getNodeListCopy());
    _aHandler.resetNodeList();
#endif
	    
}

////////////////////////////////////////////////////////////////////////
// AggregateMgr::GroupByHandler
////////////////////////////////////////////////////////////////////////
void AggregateMgr::GroupByHandler::operator()(antlr::RefAST a) {
    //std::cout << "Got GroupBy: " << walkTreeString(a) << std::endl;
    _isFrozen = true;
}
void AggregateMgr::GroupByHandler::addColumn(NodeBound const& n) {
    if(!_isFrozen) {
	_columns.push_back(n);
	//std::cout << "GroupBy new column: " << walkTreeString(n.first) 
	//	  << std::endl;
    } else {
        // FIXME. Need to handle this or note as an error.
	std::cout << "Don't know how to handle multiple group by clauses."
		  << std::endl;
    }
}
std::string AggregateMgr::GroupByHandler::getGroupByString() const {
    NodeList::const_iterator i;
    NodeList::const_iterator end = _columns.end();
    bool written = false;
    std::stringstream ss;
    ss << "GROUP BY ";
    for(i = _columns.begin(); i != end; ++i) {
	if(written) ss << ",";
	ss << "`" << walkBoundedTreeString(i->first, i->second)
	   << "`";
    }
    return ss.str();
}
////////////////////////////////////////////////////////////////////////
// AggregateMgr::GroupColumnHandler
////////////////////////////////////////////////////////////////////////
void AggregateMgr::GroupColumnHandler::operator()(antlr::RefAST a) {
    h.addColumn(NodeBound(a, getLastSibling(a)));
}

////////////////////////////////////////////////////////////////////////
// AggregateMgr
////////////////////////////////////////////////////////////////////////
AggregateMgr::AggregateMgr() : _aliaser(new AliasHandler()),
			       _setFuncer(new SetFuncHandler()),
			       _selectLister(new SelectListHandler(*_aliaser)),
			       _groupByer(new GroupByHandler()),
			       _groupColumner(new GroupColumnHandler(*_groupByer)),
			       _hasAggregate(false), _isMissingSelect(false) {
}

void AggregateMgr::postprocess() {
    AliasHandler::Map const& aMap = _aliaser->getInvAliases();
    AliasHandler::MapConstIter aEnd = aMap.end();
    SetFuncHandler::Deque const& aggd = _setFuncer->getAggs();
    
    for(SetFuncHandler::DequeConstIter i = aggd.begin(); 
	i != aggd.end(); ++i) {
	AliasHandler::MapConstIter f = aMap.find(i->first);
	std::string agg = tokenText(i->first);
	if(f != aEnd) {
	    //std::cout << agg << " aliased as " 
	    //<< tokenText(f->second.first) << std::endl;
	    SetFuncHandler::Map& m = _setFuncer->getProcs();
	    AggregateRecord a = (*m[agg])(f->second, *i);
	    //a.printTo(std::cout) << std::endl;
	    _aggRecords[i->first] = a;
		
	} else {
	    SetFuncHandler::Map& m = _setFuncer->getProcs();
	    AggregateRecord a = (*m[agg])(*i, *i);
	    //a.printTo(std::cout) << std::endl;
	    _aggRecords[i->first] = a;
	}
    }
}

void AggregateMgr::applyAggPass() {
    _isMissingSelect = true;
    std::string passText = getPassSelect();
    if(passText.empty() || (passText == "*")) {
	// SELECT * means we don't have to fix anything.
	return;
    }
    NodeBound const& nb = _selectLister->firstSelectBound;
    antlr::RefAST orphans = collapseNodeRange(nb.first, nb.second);
    nb.first->setText(passText); // Reassign text.
    nb.first->setFirstChild(antlr::RefAST()); // Set as childless.
}

std::string AggregateMgr::getPassSelect() {
    if(_isMissingSelect && _passSelect.empty()) {
	_computeSelects();
    }
    return _passSelect;
}

std::string AggregateMgr::getFixupSelect() {
    if(!_isMissingSelect && _fixupSelect.empty()) {
	_computeSelects();
    }
    return _fixupSelect;	
}

std::string AggregateMgr::getFixupPost() {
    // Fixup suffix will be ready when the fixup select is ready.
    if(!_isMissingSelect && _fixupSelect.empty()) { 
	_computeSelects();
    }
    return _fixupPost;
	
}
void AggregateMgr::_computeSelects() {
    // passSelect = "".join(map(lambda s: s.pass, selectList))
    // fixupSelect = "".join(map(lambda s: s.fixup, selectList))
    if(_selectLister->isStarFirst) {
	_passSelect = "*";
	_fixupSelect = "*";
	_hasAggregate = false;
	return;
    }
    SelectListHandler::Deque& d = _selectLister->selectLists;
    if(d.empty()) {
        _isMissingSelect = true;
        return;
    }
    assert(!d.empty());
    if(d.size() > 1) {
	std::cout << "Warning, multiple select lists->subqueries?" 
		  << std::endl; // FIXME. Should be sterner?
    }
    std::stringstream ps;
    std::stringstream fs;
    NodeList& sl = d[0];
    bool written = false;
    for(NodeListConstIter i=sl.begin(); i != sl.end(); ++i) {
	NodeBound const& nb = *i;
	// get the pass record.
	if(written) {
	    ps << ", ";
	    fs << ", ";
	} else { 
	    written = true;
	}
	if(_aggRecords.find(nb.first) != _aggRecords.end()) {
	    ps << _aggRecords[nb.first].pass;
	    fs << _aggRecords[nb.first].fixup;
	    _hasAggregate = true;
	} else {
	    std::string nonAgg = walkBoundedTreeString(nb.first, nb.second);
	    ps << nonAgg;
	    fs << "`"  << nonAgg << "`"; // Safe to quote.
	}
	
    } 
    _computePost();
    _passSelect = ps.str();
    _fixupSelect = fs.str();
}

void AggregateMgr::_computePost() {
    // For now, only handle group by.
    if(_groupByer->getHasColumns()) {
	_fixupPost = _groupByer->getGroupByString();
    } else {
	_fixupPost = ""; 
    }
}
