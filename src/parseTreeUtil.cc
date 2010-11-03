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

/// parseTreeUtil.cc - non-templated implementation bits for working with
/// antlr parse trees.

#include "lsst/qserv/master/parseTreeUtil.h"

#include <antlr/ASTFactory.hpp>

namespace qMaster=lsst::qserv::master;

// Creates a new text node and and puts it into the tree
// after the specified node, but before the node's next sibling.
antlr::RefAST qMaster::insertTextNodeAfter(antlr::ASTFactory* factory, 
                                           std::string const& s, 
                                           antlr::RefAST n) {
#if 1
    antlr::RefAST newChild = factory->create();
    newChild->setText(s); 
    newChild->setNextSibling(n->getNextSibling());
    n->setNextSibling(newChild);
    return n;
#else
#endif
}

// Overwrites the text for the specified node, putting the old text into
// a new text node placed after the specified node but before the
// node's next sibling.
antlr::RefAST qMaster::insertTextNodeBefore(antlr::ASTFactory* factory, 
                                            std::string const& s, 
                                            antlr::RefAST n) {
#if 0
    std::string oldText = n->getText();
    n->setText(s);
    return insertTextNodeAfter(factory, oldText, n);
#else
   antlr::RefAST newChild = factory->create();
   newChild->setText(n->getText());
   newChild->setNextSibling(n->getNextSibling());
   n->setNextSibling(newChild);
   n->setText(s);
   n = newChild;
   return n;
#endif
}

