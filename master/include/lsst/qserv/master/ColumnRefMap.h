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
#ifndef LSST_QSERV_MASTER_COLUMNREFMAP_H
#define LSST_QSERV_MASTER_COLUMNREFMAP_H
/**
  * @file ColumnRefMap.h
  *
  * @brief ColumnRefMap is a parse handler that maintains a mapping of parse
  * nodes to ColumnRef objects.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <ostream>
#include "lsst/qserv/master/ColumnRefH.h"
namespace lsst {
namespace qserv {
namespace master {

class ColumnRef; // Forward

/// ColumnRefMap is a listener that maintains a mapping from node to a
/// ColumnRef. Consider eliminating ColumnRefNodeMap (ColumnRefH.h)

class ColumnRefMap : public ColumnRefH::Listener {
public:
    typedef boost::shared_ptr<ColumnRef> RefPtr;
    typedef std::list<RefPtr> List;

    ColumnRefMap() {}
    virtual ~ColumnRefMap() {}
    virtual void acceptColumnRef(antlr::RefAST d, antlr::RefAST t,
                                 antlr::RefAST c);

    boost::shared_ptr<ColumnRef const> getRef(antlr::RefAST r) const;
    std::ostream& printRefs(std::ostream& os) const;

private:
    typedef std::map<antlr::RefAST, boost::shared_ptr<ColumnRef> > RefMap;
    RefMap _refs;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_COLUMNREFMAP_H
