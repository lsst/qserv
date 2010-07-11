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
 
#ifndef LSST_QSERV_MASTER_PARSETREEUTIL_H
#define LSST_QSERV_MASTER_PARSETREEUTIL_H
#include <cassert>
#include <iostream>
#include <map>

#include "antlr/AST.hpp"

namespace lsst {
namespace qserv {
namespace master {

template <typename AnAst>
std::string tokenText(AnAst r) {
    if(r.get()) {
	return r->getText();
    } else return std::string();
}

template <typename AnAst>
struct TrivialCheckTerm {
    bool operator()(AnAst r, int depth) {return false; }
};

template <typename AnAst>
struct ParenCheckTerm {
    bool operator()(AnAst r, int depth) {
	return (depth == 0) && (tokenText(r) == ")");
    }
};

template <typename AnAst>
struct SibCheckTerm {
    SibCheckTerm(AnAst lastSib_) : lastSib(lastSib_) {}
    bool operator()(AnAst r, int depth) {
	return (depth == 0) && (r == lastSib);
    }
    AnAst lastSib;
};

template <typename AnAst>
struct PrintVisitor {
public:
    void operator()(AnAst a) {
	if(!result.empty()) {
	    result += " " + a->getText();
	} else {
	    result = a->getText();
	}
    }
    std::string result;
};

template <typename AnAst>
struct CompactPrintVisitor {
public:
    void operator()(AnAst a) {
	std::string s = a->getText();
	if(!s.empty() && !result.empty()) {
	    int last = result[result.size()-1];
	    int next = s[0];
	    if(shouldSeparate(last,next)) {
		result += " ";
	    } 
	}
	result += s;
    }
    bool shouldSeparate(int last, int next) {
	return (isalnum(last) && isalnum(next)) // adjoining alnums
	    || ((last == '*') && isalnum(next)) // *saf
	    || ((next == '*') && isalnum(last)) // saf*
	    || ((last == ')') && isalnum(next)) // )asdf
	    ;
    }
    std::string result;
};

template <typename AnAst>
std::string walkTree(AnAst r) {
    //DFS walk
    // Print child (child will print siblings)
    std::string result;
    antlr::RefAST c = r->getFirstChild();
    if(c.get()) {
	result = walkTree(c);
    }
    // Now print sibling(s)
    antlr::RefAST s = r->getNextSibling();
    if(s.get()) {
	if(!result.empty()) result += " ";
	result += walkTree(s);
    }
    if(!result.empty()) result = " " + result;
    return r->getText() + result;
	
}

template <typename AnAst, typename Visitor, typename CheckTerm>
void walkTreeVisit(AnAst r, Visitor& v, CheckTerm& ct, int depth=0) {
    //DFS walk?
    v(r);
    //if(ct(r,depth)) return; // On terminal, visit only.
    antlr::RefAST c = r->getFirstChild();
    if(c.get()) {
	std::cout << "Child: " << tokenText(r) << "----" << tokenText(c) 
		  << std::endl;
	walkTreeVisit(c, v, ct, depth+1);
    }
    // Now print sibling(s)
    antlr::RefAST s = r->getNextSibling();
    if(s.get() && !ct(r,depth)) {
	//	std::cout << "Sib: " << tokenText(r) << "----" << tokenText(s) 
	//		  << std::endl;
	walkTreeVisit(s, v, ct, depth);
    }
	
}

template <typename AnAst, typename Visitor>
void walkTreeVisit(AnAst r, Visitor& v) {
    TrivialCheckTerm<AnAst> t;
    walkTreeVisit(r, v, t);
}

template <typename AnAst>
std::string walkTreeString(AnAst r) {
    CompactPrintVisitor<AnAst> p;
    TrivialCheckTerm<AnAst> t;
    walkTreeVisit(r, p, t);
    return p.result;
}

template <typename AnAst>
std::string walkBoundedTreeString(AnAst r, AnAst lastSib) {
    CompactPrintVisitor<AnAst> p;
    SibCheckTerm<AnAst> t(lastSib);
    walkTreeVisit(r, p, t);
    return p.result;
}


template <typename AnAst>
std::string getFuncString(AnAst r) {
    CompactPrintVisitor<AnAst> p;
    ParenCheckTerm<AnAst> t;
    walkTreeVisit(r, p, t);
    return p.result;
}

template <typename AnAst>
AnAst getLastSibling(AnAst r) {
    AnAst last;
    do {
	last = r;
	r = r->getNextSibling();
    } while(r.get());
    return last;
}

template <typename AnAst>
AnAst collapseNodeRange(AnAst start, AnAst bound) {
    // Destroy a node's siblings stopping (but including) a bound
    // This is useful in patching up an AST, substituting one parse 
    // element for another. 
    // @return the missing fragment so the caller can save it.

    // Example:
    // RefAST someList;
    // RefAST listBound = getLastSibling(someList)
    // someList->setTokenText("Something new")
    // collapseNodeRange(someList, listBound)
    
    // This is a simple linked-list ranged delete.
    assert(start.get());
    assert(bound.get());
    AnAst dead = start->getNextSibling();
    start->setNextSibling(bound->getNextSibling());
    return dead;
}


}}} // lsst::qserv::master

#endif // LSST_QSERV_MASTER_PARSETREEUTIL_H
