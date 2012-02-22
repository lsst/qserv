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
#include "lsst/qserv/master/TableRefChecker.h"
#include "lsst/qserv/master/common.h"

using lsst::qserv::master::Templater;
using lsst::qserv::master::TableRefChecker;

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
    //    walkTreeVisit(a, _deferred); // Defer so that the alias processing
                                 // can happen first.
}

void Templater::TableListHandler::processJoin() {
    JoinVisitor j(_templater.getDelimiter(), "_sc");
    _deferred.visit(j);
    j.applySubChunkRule();
    _hasChunks = j.getHasChunks();
    _hasSubChunks = j.getHasSubChunks();
    _usageCount = j.getUsageCount();
}

////////////////////////////////////////////////////////////////////////
// Templater::addAliasFunc
////////////////////////////////////////////////////////////////////////
void Templater::addAliasFunc::operator()(std::string const& aName) {
    //std::cout << "Accepting alias: " << aName << std::endl;
    _t._tableAliases[aName] = 1;
}

////////////////////////////////////////////////////////////////////////
// Templater
////////////////////////////////////////////////////////////////////////
std::string const Templater::_nameSep(".");

Templater::Templater(std::string const& delimiter, 
                     antlr::ASTFactory* factory)
    :  _delimiter(delimiter),
       _factory(factory),
       _fromStmtActive(false), _shouldDefer(true) {
}
void Templater::setup(Templater::IntMap const& dbWhiteList,
                      boost::shared_ptr<TableRefChecker const> refChecker,
                      std::string const& defaultDb) {
    _dbWhiteList = dbWhiteList;
    _refChecker = refChecker;
    _defaultDb = defaultDb;    
}

void Templater::processNames() {
    for(RefPairQueue::iterator i=_tableProcessQueue.begin();
        i != _tableProcessQueue.end(); ++i) {
        _processName(*i);
    }
    for(RefPairQueue::iterator i=_columnProcessQueue.begin();
        i != _columnProcessQueue.end(); ++i) {
        _processName(*i);
    }

    _tableProcessQueue.clear();
    _columnProcessQueue.clear();
#if 0
    JoinVisitor j(_templater.getDelimiter(), "_sc");
    _deferred.visit(j);
    j.applySubChunkRule();
    _hasChunks = j.getHasChunks();
    _hasSubChunks = j.getHasSubChunks();
    _usageCount = j.getUsageCount();
#endif
}

bool Templater::_isAlias(std::string const& alias) {
    return 1 == getFromMap(_tableAliases, alias, 0);
}

void Templater::_processLater(antlr::RefAST db, antlr::RefAST n) {
    RefAstPair p(db, n);
    if(_shouldDefer) {
        if(_fromStmtActive) {
            _tableProcessQueue.push_back(p);
        } else { 
            _columnProcessQueue.push_back(p);
        }
    } else {
        _processName(p);
    }
}

void Templater::_processName(Templater::RefAstPair& dbn) {
    // n: first= db name node
    // second=table name node
    antlr::RefAST db = dbn.first;
    antlr::RefAST n = dbn.second;
    std::string dbName;
    std::string tableName = n->getText();
    if(!db.get()) {
        // Check if alias.  If so, do not touch.
        //std::cout << "PROCESS: " << tableName << std::endl;
        if(_isAlias(tableName)) return; // Don't process aliases.
        //else std::cout << "non-alias: " << tableName << std::endl;
        if(!_defaultDb.empty() && _isDbOk(_defaultDb)) {
            // no explicit Db?  Create one, and link it in.
            n = insertTextNodeBefore(_factory, _defaultDb + _nameSep, n);
            dbName = _defaultDb;
        } else { // No context and bad/missing defaultDb
            //std::cout << _defaultDb << " is bad(default)." << std::endl;
            _markBadDb(_defaultDb);
        }
    } else {
        dbName = db->getText();
        if(!_isDbOk(dbName)) {
            _markBadDb(dbName);
        }
    }
    if(_refChecker->isChunked(dbName, tableName)) {
        std::string mungedName = mungeName(tableName);        
        n->setText(mungedName);
    }
}

void Templater::signalFromStmtBegin() {
    _fromStmtActive = true;
}
void Templater::signalFromStmtEnd() {
    _fromStmtActive = false;
    _shouldDefer = false;
}

////////////////////////////////////////////////////////////////////////
bool Templater::_isDbOk(std::string const& db) {
    return _dbWhiteList.end() != _dbWhiteList.find(db);
}

////////////////////////////////////////////////////////////////////////
void Templater::_markBadDb(std::string const& db) {
    _badDbs.push_back(db);
}
