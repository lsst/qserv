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
// FromFactory constructs a FromList that maintains parse state of
// the FROM clause for future interrogation, manipulation, and
// reconstruction.
#ifndef LSST_QSERV_MASTER_FROMFACTORY_H
#define LSST_QSERV_MASTER_FROMFACTORY_H
/**
  * @file FromFactory.h
  *
  * @brief FromFactory is a factory for constructing FromLists from
  * ANTLR nodes.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <antlr/AST.hpp>
#include <boost/shared_ptr.hpp>

// Impl
#include <boost/make_shared.hpp>

// Forward
class SqlSQL2Parser;

namespace lsst {
namespace qserv {
namespace master {
// Forward
class ParseAliasMap;
class FromList;

class FromFactory {
public:
    friend class SelectFactory;
    class TableRefListH;
    class TableRefAuxH;
    friend class TableRefListH;
    class RefGenerator;

    FromFactory(boost::shared_ptr<ParseAliasMap> aliases);
    boost::shared_ptr<FromList> getProduct();
private:
    void attachTo(SqlSQL2Parser& p);
    void _import(antlr::RefAST a);

    boost::shared_ptr<ParseAliasMap> _aliases;
    boost::shared_ptr<FromList> _list;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_FROMFACTORY_H

