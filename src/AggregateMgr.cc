#include <sstream>
#include "lsst/qserv/master/AggregateMgr.h"
using lsst::qserv::master::AggregateMgr;
using lsst::qserv::master::AggregateRecord;
using lsst::qserv::master::NodeBound;


////////////////////////////////////////////////////////////////////////
// AggregateMgr::EasyAggBuilder
////////////////////////////////////////////////////////////////////////
AggregateRecord AggregateMgr::EasyAggBuilder::operator()(NodeBound const& lbl,
							 NodeBound const& meaning) {
    AggregateRecord a;
    a.lbl = lbl;
    a.meaning = meaning;
    if(lbl.first != meaning.first) {
	assert(lbl.second.get()); // must have bound.
		a.orig = walkBoundedTreeString(meaning.first, lbl.second);
    } else {
	a.orig = walkBoundedTreeString(meaning.first, meaning.second);
    }
    a.pass = a.orig;
    a.fixup = computeFixup(meaning, lbl);
    return a;
}


std::string AggregateMgr::EasyAggBuilder::computeFixup(NodeBound meaning, 
						       NodeBound lbl) {
    std::string agg = tokenText(meaning.first);
    antlr::RefAST lparen = meaning.first->getNextSibling();
    assert(lparen.get());
    antlr::RefAST paramAst = lparen->getNextSibling();
    assert(paramAst.get());
    std::string param = getFuncString(paramAst);
    std::string lblText = walkBoundedTreeString(lbl.first, lbl.second);
    // Orig: agg ( param ) lbl
    // Fixup: agg ( quoted-lbl) 
    return agg + "(`" + lblText + "`) AS `" + lblText + "`";
}

////////////////////////////////////////////////////////////////////////
// AggregateMgr::SetFuncHandler
////////////////////////////////////////////////////////////////////////
AggregateMgr::SetFuncHandler::SetFuncHandler() {
	    _map["count"].reset();
	    _map["avg"].reset();
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
// AggregateMgr
////////////////////////////////////////////////////////////////////////
AggregateMgr::AggregateMgr() : _aliaser(new AliasHandler()),
			       _setFuncer(new SetFuncHandler()),
			       _selectLister(new SelectListHandler(*_aliaser)) {
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
    std::string passText = getPassSelect();
    if(passText == "*") {
	// SELECT * means we don't have to fix anything.
	return;
    }
    NodeBound const& nb = _selectLister->firstSelectBound;
    antlr::RefAST orphans = collapseNodeRange(nb.first, nb.second);
    nb.first->setText(passText);
}
std::string AggregateMgr::getPassSelect() {
    if(_passSelect.empty()) {
	_computeSelects();
    }
    return _passSelect;
}
std::string AggregateMgr::getFixupSelect() {
    if(_fixupSelect.empty()) {
	_computeSelects();
    }
    return _fixupSelect;
	
}
void AggregateMgr::_computeSelects() {
    // passSelect = "".join(map(lambda s: s.pass, selectList))
    // fixupSelect = "".join(map(lambda s: s.fixup, selectList))
    if(_selectLister->isStarFirst) {
	_passSelect = "*";
	_fixupSelect = "*";
	return;
    }
    SelectListHandler::Deque& d = _selectLister->selectLists;
    assert(!d.empty());
    if(d.size() > 1) {
	std::cout << "Warning, multiple select lists->subqueries?" 
		  << std::endl; // FIXME. Should be sterner?
    }
    std::stringstream ps;
    std::stringstream fs;
    NodeList& sl = d[0];
    bool written = false;
    for(NodeListConstIter i=sl.begin();
	i != sl.end(); ++i) {
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
	
	} else {
	    std::string nonAgg = walkBoundedTreeString(nb.first, nb.second);
	    ps << nonAgg;
	    fs << "`"  << nonAgg << "`"; // Safe to quote.
	}
	
    } 
    _passSelect = ps.str();
    _fixupSelect = fs.str();
	
    
}


