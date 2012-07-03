// -*- LSST-C++ -*-
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
// ColumnRefH -- column reference parse handler for parser model 3
#ifndef LSST_QSERV_MASTER_COLUMNREFH_H
#define LSST_QSERV_MASTER_COLUMNREFH_H

// Standard
#include <list>

#include "lsst/qserv/master/parserBase.h" // VoidFourRefFunc
#include "lsst/qserv/master/parseTreeUtil.h" // tokenText
// Forward
class SqlSQL2Parser;

namespace lsst {
namespace qserv {
namespace master {

class ColumnRefH : public VoidFourRefFunc {
public: 
    class Listener;
    typedef boost::shared_ptr<ColumnRefH> Ptr;
    ColumnRefH() {}
    virtual ~ColumnRefH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b, 
                            antlr::RefAST c, antlr::RefAST d) {
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

class ColumnRefH::Listener {
public:
    virtual ~Listener() {}
    virtual void acceptColumnRef(antlr::RefAST d, antlr::RefAST t, 
                                 antlr::RefAST c) = 0;
};

class ColumnRefMap : public ColumnRefH::Listener {
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
inline void 
ColumnRefH::_process(antlr::RefAST d, antlr::RefAST t, antlr::RefAST c) {
    using lsst::qserv::master::tokenText;
    // std::cout << "columnref: db:" << tokenText(d)
    //           << " table:" << tokenText(t)
    //           << " column:" << tokenText(c) << std::endl;
    if(_listener.get()) { _listener->acceptColumnRef(d, t, c); }
}


}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_COLUMNREFH_H

