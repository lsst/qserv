// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
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
#ifndef LSST_QSERV_PARSER_PARSETREEUTIL_H
#define LSST_QSERV_PARSER_PARSETREEUTIL_H
/**
  * @file
  *
  * @brief Utility functions for examining,  processing, and
  * manipulating the ANTLR parse tree.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <cassert>
#include <iostream>
#include <list>
#include <map>
#include <sstream>

// Third-party headers
#include "antlr/AST.hpp"

// Local headers
#include "global/sqltoken.h"
#include "global/stringTypes.h"

// Forward
namespace antlr {
class ASTFactory;
}

namespace lsst {
namespace qserv {
namespace parser {

template <typename AnAst>
std::string tokenText(AnAst const &r) {
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
	    if(sql::sqlShouldSeparate(lastToken, last,next)) {
		result += " ";
	    }
	}
        lastToken = s;
	result += s;
    }
    std::string lastToken;
    std::string result;
};

bool substituteWithMap(std::string& s,
                       std::map<std::string, std::string> const& m,
                       int minMatch);

template <typename AnAst>
struct SubstituteVisitor {
public:
    SubstituteVisitor(StringMap const& m_) : m(m_) {
        std::string::size_type min = std::string::npos;
        StringMap::const_iterator i;
        for(i=m.begin(); i != m.end(); ++i) {
            if(i->first.size() < min) min = i->first.size();
        }
        minMatch = min;
    }

    void operator()(AnAst a) {
	std::string s = a->getText();
        if(substituteWithMap(s, m, minMatch)) a->setText(s);
    }
    StringMap const& m;
    int minMatch;
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
    AnAst first = r;
    do {
        // DFS walk
        v(r);
        antlr::RefAST c = r->getFirstChild();
        if(c.get()) {
            walkTreeVisit(c, v, ct, depth+1);
        }
        r = r->getNextSibling();
    } while(r.get() && !ct(r,depth));
}
template <typename AnAst, typename C>
class IndentPrinter {
public:
    IndentPrinter(std::ostream& o_) : o(o_) {}
    void operator()(AnAst const &a, C& p) {
        o << p.size() << std::string(p.size(), ' ') << tokenText(a) << std::endl;
    }
    std::ostream& o;
};

// AnAST: e.g. RefAST
// V: Visitor: implements void operator()(AnAST, C const&)
// C: Container of AnAst, e.g. std::list<RefAST>
template <typename AnAst, typename V, typename C>
void visitTreeRooted(AnAst const &r, V& v, C& p) {
    //DFS walk
    antlr::RefAST s = r;
    while(s.get()) {
        //--- (visit self)---
        v(s, p);
        antlr::RefAST c = s->getFirstChild();
        if(c.get()) {
            p.push_back(s);
            visitTreeRooted(c, v, p);
            p.pop_back();
        }
        s = s->getNextSibling();
    }
}

template <typename AnAst>
void printIndented(AnAst r) {
    std::vector<AnAst> vector;
    IndentPrinter<AnAst, std::vector<AnAst> > p(std::cout);
    visitTreeRooted(r, p, vector);
}

template <typename AnAst>
std::string walkIndentedString(AnAst r) {
    std::list<AnAst> mylist;
    std::stringstream ss;
    IndentPrinter<AnAst, std::list<AnAst> > p(ss);
    visitTreeRooted(r, p, mylist);
    return ss.str();
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
std::string walkSiblingString(AnAst r) {
    CompactPrintVisitor<AnAst> p;
    while(r.get()) {
        p(r);
        r = r->getNextSibling();
    }
    return p.result;
}


template <typename AnAst>
void walkTreeSubstitute(AnAst r,
                        std::map<std::string, std::string> const& m) {
    SubstituteVisitor<AnAst> s(m);
    walkTreeVisit(r, s);
}


template <typename AnAst, typename Check>
AnAst findSibling(AnAst r, Check& c) {
    while(r.get()) {
        if(c(r)) { break; }
        r = r->getNextSibling();
    }
    return r;
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
AnAst getSiblingBefore(AnAst r, AnAst b) {
    AnAst last;
    do {

	last = r;
	r = r->getNextSibling();

    } while(r != b);
    return last;
}


template <typename AnAst>
int countLength(AnAst r, AnAst b) {
    int i = 0;
    while(r.get() && (r != b)) {
        ++i;
	r = r->getNextSibling();
    }
    return i;
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

template <typename AnAst>
AnAst collapseToSingle(AnAst start) {
    AnAst listBound = getLastSibling(start);
    AnAst dead = collapseNodeRange(start, listBound);
    return dead;
}

// Creates a new text node and and puts it into the tree
// after the specified node, but before the node's next sibling.
antlr::RefAST insertTextNodeAfter(antlr::ASTFactory* factory,
                                  std::string const& s,
                                  antlr::RefAST n);

// Overwrites the text for the specified node, putting the old text into
// a new text node placed after the specified node but before the
// node's next sibling.
antlr::RefAST insertTextNodeBefore(antlr::ASTFactory* factory,
                                  std::string const& s,
                                   antlr::RefAST n);

void printDigraph(std::string lbl, std::ostream& o, antlr::RefAST n);

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_PARSETREEUTIL_H
