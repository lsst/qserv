/*
 * LSST Data Management System
 * Copyright 2009-2013 LSST Corporation.
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
/**
  * @file parseTreeUtil.cc
  *
  * @brief  non-templated implementation bits for working with ANTLR
  * parse trees.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "parser/parseTreeUtil.h"
#include <stdexcept>
#include <antlr/ASTFactory.hpp>


namespace {
template <typename AnAst, typename C>
class DigraphVisitor {
public:
    typedef std::map<AnAst, std::string> AstMap;

    DigraphVisitor() : i(0) {}
    void operator()(AnAst a, C& p) {
        std::string parent = stringify(p.back());
        s << "\"" << parent << "\"" << " -> "
          << "\"" << stringify(a) << "\"" << "\n";
    }
    std::string stringify(AnAst a) {
        typename AstMap::const_iterator e(ids.end());
        if(ids.find(a) == e) {
            std::stringstream t;
            t << lsst::qserv::parser::tokenText(a) << "[" << ++i << "]";
            ids[a] = t.str();
        }
        return ids[a];
    }
    void output(std::string label, std::ostream& o) {
        o << "digraph tree_" << label << " {\n"
          << s.str()
          << "}\n";
    }
    int i;
    AstMap ids;
    std::stringstream s;

};
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace parser {
        
// Creates a new text node and and puts it into the tree
// after the specified node, but before the node's next sibling.
antlr::RefAST
insertTextNodeAfter(antlr::ASTFactory* factory,
                    std::string const& s,
                    antlr::RefAST n) {
    antlr::RefAST newChild = factory->create();
    newChild->setText(s);
    newChild->setNextSibling(n->getNextSibling());
    n->setNextSibling(newChild);
    return n;
}

// Overwrites the text for the specified node, putting the old text into
// a new text node placed after the specified node but before the
// node's next sibling.
antlr::RefAST
insertTextNodeBefore(antlr::ASTFactory* factory,
                     std::string const& s,
                     antlr::RefAST n) {
   antlr::RefAST newChild = factory->create();
   newChild->setText(n->getText());
   newChild->setNextSibling(n->getNextSibling());
   n->setNextSibling(newChild);
   n->setText(s);
   n = newChild;
   return n;
}

void 
printDigraph(std::string lbl, std::ostream& o, antlr::RefAST n) {
    DigraphVisitor<antlr::RefAST, std::list<antlr::RefAST> > dv;
    std::list<antlr::RefAST> c;
    visitTreeRooted(n, dv, c);
    dv.output(lbl, o);
}

bool
substituteWithMap(std::string& s,
                  std::map<std::string, std::string>  const& m,
                  int minMatch) {
    if(s.empty()) return false;
    if(minMatch < 0) {
        throw std::invalid_argument("substituteWithMap needs minMatch >= 0");
    }
    bool did = false;
    std::map<std::string, std::string>::const_iterator i = m.find(s);
    if(i != m.end()) {
        s = i->second;
        did = true;
    } else if(s.size() >= static_cast<unsigned>(minMatch)) {
        // more aggressively for larger tokens.
        for(i=m.begin(); i != m.end(); ++i) {
            std::string orig = i->first;
            std::string repl = i->second;
            for(std::string::size_type j=0; j < s.size(); ++j) {
                std::string::size_type f = s.find(orig, j);
                if(f == std::string::npos) break;
                s.replace(f, orig.size(), repl);
                j += repl.size() - 1;
                did = true;
            }  // for all matches
        } // for substitutions in map
    } // if original substitute failed.
    return did;
}

}}} // namespace lsst::qserv::parser
