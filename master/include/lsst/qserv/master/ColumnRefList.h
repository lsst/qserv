// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_COLUMNREFLIST_H
#define LSST_QSERV_MASTER_COLUMNREFLIST_H
/**
  * @file ColumnRefList.h
  *
  * @brief ColumnRefList is a list of column refs derived from a sql parse
  * tree. 
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/ColumnRefList.h"
#include "lsst/qserv/master/ColumnRefH.h"

namespace lsst { namespace qserv { namespace master {
class ColumnRef; // Forward

/// ColumnRefList is a listener that maintains a mapping from node to a
/// ColumnRef. Consider eliminating ColumnRefMap (ColumnRefH.h)
class ColumnRefList : public ColumnRefH::Listener {
public:
    ColumnRefList() {}
    virtual ~ColumnRefList() {}
    virtual void acceptColumnRef(antlr::RefAST d, antlr::RefAST t, 
                                 antlr::RefAST c);
    boost::shared_ptr<ColumnRef const> getRef(antlr::RefAST r);
    void printRefs() const;

private:
    typedef std::map<antlr::RefAST, boost::shared_ptr<ColumnRef> > RefMap;
    RefMap _refs;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_COLUMNREFLIST_H

