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
 
#include <sstream>
#include <list>

#include <antlr/ASTFactory.hpp>

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

//
class ImplicitDbVisitor {
    public:
    ImplicitDbVisitor() {
        std::cout << "newVisit" << std::endl;
}
    void operator()(antlr::RefAST& a) {
        std::cout << "dbVisit: " << a->getText() << std::endl;
        lastRefs.push_back(a);
        if(!isName(a)) {
            return;
        }
        std::cout << "table name!" << std::endl;
        // check for db . table
        RefList::reverse_iterator i = lastRefs.rbegin();
        
    }

    inline bool isAlpha(char c) {
        return (('a' <= c) && (c <= 'z')) // lower case
            || (('A' <= c) && (c <= 'Z')); // upper case
    }

    inline bool isValidChar(char c) {
        return ((('a' <= c) && (c <= 'z')) // lower case
                || (('A' <= c) && (c <= 'Z')) // upper case
                || (('0' <= c) && (c <= '9')) // digit
                || (c == '_') // underscore
                || (c == '$')); // dollar sign
    }

    bool isName(antlr::RefAST const& a) {
        std::string t = a->getText();
        char const* p;
        bool charsOk = true;
        bool hasAlpha = false;
        for(p = t.c_str(); charsOk && *p; ++p) {
            //std::cout << "->" << *p << std::endl;
            charsOk &= isValidChar(*p);
        } 
        if(!charsOk) return false;
        for(p = t.c_str(); *p && !hasAlpha; ++p) {
            hasAlpha |= isAlpha(*p);
        }
        return hasAlpha;
        // 
    }

    typedef std::list<antlr::RefAST> RefList;
    RefList lastRefs;
};



////////////////////////////////////////////////////////////////////////
// Templater::TableListHandler
////////////////////////////////////////////////////////////////////////
void Templater::TableListHandler::operator()(antlr::RefAST a, 
					     antlr::RefAST b) {
    JoinVisitor j(_templater.getDelimiter(), "_sc");
    //ImplicitDbVisitor v;
    //walkTreeVisit(a,v);
    walkTreeVisit(a,j);
    j.applySubChunkRule();
    _hasChunks = j.getHasChunks();
    _hasSubChunks = j.getHasSubChunks();
    _usageCount = j.getUsageCount();
}

////////////////////////////////////////////////////////////////////////
// Templater
////////////////////////////////////////////////////////////////////////
std::string const Templater::_nameSep(".");

Templater::Templater(std::string const& delimiter, 
                     antlr::ASTFactory* factory,
                     Templater::IntMap const& dbWhiteList,
                     std::string const& defaultDb) 
    : _dbWhiteList(dbWhiteList), _delimiter(delimiter),
      _factory(factory), _defaultDb(defaultDb) {
}

void Templater::_processName(antlr::RefAST db, antlr::RefAST n) {
    if(!db.get()) {
        if(!_defaultDb.empty() && _isDbOk(_defaultDb)) {
            // no explicit Db?  Create one, and link it in.
            n = insertTextNodeBefore(_factory, _defaultDb + _nameSep, n);
        } else { // No context and bad/missing defaultDb
            _markBadDb(_defaultDb);
        }
    } else {
        std::string dbStr = db->getText();
        if(!_isDbOk(dbStr)) {
            _markBadDb(dbStr);
        }
    }
    if(isSpecial(n->getText())) {
        n->setText(mungeName(n->getText()));
    }
}

////////////////////////////////////////////////////////////////////////
bool Templater::_isDbOk(std::string const& db) {
    return _dbWhiteList.end() != _dbWhiteList.find(db);
}

////////////////////////////////////////////////////////////////////////
void Templater::_markBadDb(std::string const& db) {
    _badDbs.push_back(db);
}
