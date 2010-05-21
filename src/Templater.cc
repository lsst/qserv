#include <sstream>
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/Templater.h"

using lsst::qserv::master::Templater;

////////////////////////////////////////////////////////////////////////
// Templater::JoinVisitor
////////////////////////////////////////////////////////////////////////
void Templater::JoinVisitor::operator()(antlr::RefAST& a) {
    if(_isDelimited(a->getText())) {
	_addRef(a);
	_hasChunks = true;
    }
}
void Templater::JoinVisitor::applySubChunkRule() {
    RefMapIter e = _map.end();
    for(RefMapIter i = _map.begin(); i != e; ++i) {
	if(i->second.size() > 1) {
	    _reassignRefs(i->second);
	    _hasSubChunks = true;
	}
    }
}
Templater::IntMap Templater::JoinVisitor::getUsageCount() const{
    IntMap im;
    RefMapConstIter e = _map.end();
    register int delimLength = _delim.length();
    register int delimLength2 = delimLength + delimLength;
    for(RefMapConstIter i = _map.begin(); i != e; ++i) {
	RefMapValue const& v = *i;
	std::string key = v.first.substr(delimLength, 
					 v.first.length()-delimLength2);
	im[key] = v.second.size();
    }
    return im;
}

void Templater::JoinVisitor::_addRef(antlr::RefAST& a) {
    std::string key(a->getText());
    if(_map.find(key) == _map.end()) {
	_map.insert(RefMap::value_type(key,RefList()));
    }
    _map[key].push_back(a);
}
bool Templater::JoinVisitor::_isDelimited(std::string const& s) {
    std::string::size_type lpos = s.find(_delim);
    if(std::string::npos != lpos && lpos == 0) {
	std::string::size_type rpos = s.rfind(_delim);
	if((std::string::npos != rpos) 
	   && (rpos == s.size()-_delim.size())) {
	    return true;
	}
    }
    return false;
}
void Templater::JoinVisitor::_reassignRefs(RefList& l) {
    RefListIter e = l.end();
    int num=1;
    for(RefListIter i = l.begin(); i != e; ++i) {
	antlr::RefAST& r = *i;
	std::string orig = r->getText();
	std::stringstream specss; 
	specss << _subPrefix << num++;
	orig.insert(orig.rfind(_delim), specss.str());
	r->setText(orig);
    }
}

////////////////////////////////////////////////////////////////////////
// Templater::TableListHandler
////////////////////////////////////////////////////////////////////////
void Templater::TableListHandler::operator()(antlr::RefAST a, 
					     antlr::RefAST b) {
    TypeVisitor t;
    JoinVisitor j("*?*", "_sc");
    walkTreeVisit(a,j);
    j.applySubChunkRule();
    _hasChunks = j.getHasChunks();
    _hasSubChunks = j.getHasSubChunks();
    _usageCount = j.getUsageCount();
}


