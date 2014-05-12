// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_PARSER_COLUMNREFH_H
#define LSST_QSERV_PARSER_COLUMNREFH_H
/**
  * @file ColumnRefH.h
  *
  * @brief  ColumnRefH is a column reference parse handler that is triggered
  * when the ANTLR parser produces column references. ColumnRefNodeMap
  * maintains node-to-parsed-ref mappings.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <list>

// Third-party include
#include <boost/shared_ptr.hpp>

// Local headers
#include "parser/parserBase.h" // VoidFourRefFunc
#include "parser/parseTreeUtil.h" // tokenText

// Forward
class SqlSQL2Parser;

namespace lsst {
namespace qserv {
namespace parser {

/// ColumnRefH is a parse action for column_ref tokens in the grammar
class ColumnRefH : public VoidFourRefFunc {
public:
    class Listener;
    typedef boost::shared_ptr<ColumnRefH> Ptr;
    ColumnRefH() {}
    virtual ~ColumnRefH() {}
    /// Accept nodes for a column ref and pass to a listener.
    virtual void operator()(antlr::RefAST a, antlr::RefAST b,
                            antlr::RefAST c, antlr::RefAST d) {
        /// The listener abstraction deals with differently-formed column
        /// references, i.e., column; table.column; database.table.column
        if(d.get()) {
            _process(b, c, d);
        } else if(c.get()) {
            _process(a, b, c);
        } else if(b.get()) {
            _process(antlr::RefAST(), a, b);
        } else {
            _process(antlr::RefAST(), antlr::RefAST(), a);
        }
    }
    void setListener(boost::shared_ptr<Listener> crl) {
        _listener = crl;
    }
private:
    inline void _process(antlr::RefAST d, antlr::RefAST t, antlr::RefAST c);
    boost::shared_ptr<Listener> _listener;
};

/// ColumnRefH::Listener is an interface that acts upon normalized
/// column references (db,table,column)
class ColumnRefH::Listener {
public:
    virtual ~Listener() {}
    virtual void acceptColumnRef(antlr::RefAST d, antlr::RefAST t,
                                 antlr::RefAST c) = 0;
};
/// ColumnRefNodeMap is a Listener which remembers ColumnRefs as nodes.
/// Somewhat different than ColumnRefMap, which stores strings rather
/// than node refs.
class ColumnRefNodeMap : public ColumnRefH::Listener {
public:
    struct Ref {
        Ref() {}
        Ref(antlr::RefAST d, antlr::RefAST t, antlr::RefAST c)
            : db(d), table(t), column(c) {}
        antlr::RefAST db;
        antlr::RefAST table;
        antlr::RefAST column;
    };
    typedef std::map<antlr::RefAST, Ref> Map;

    virtual void acceptColumnRef(antlr::RefAST d, antlr::RefAST t,
                                 antlr::RefAST c) {
        Ref r(d, t, c);
        if(d.get())      { map[d] = r; }
        else if(t.get()) { map[t] = r; }
        else             { map[c] = r; }
    }
    Map map;
};

////////////////////////////////////////////////////////////////////////
// Inlines
////////////////////////////////////////////////////////////////////////
/// Redirect a normalized ref to a listener, if available
inline void
ColumnRefH::_process(antlr::RefAST d, antlr::RefAST t, antlr::RefAST c) {
    // std::cout << "columnref: db:" << tokenText(d)
    //           << " table:" << tokenText(t)
    //           << " column:" << tokenText(c) << std::endl;
    if(_listener.get()) { _listener->acceptColumnRef(d, t, c); }
}

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_COLUMNREFH_H
