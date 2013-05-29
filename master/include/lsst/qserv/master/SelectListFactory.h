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
#ifndef LSST_QSERV_MASTER_SELECTLISTFACTORY_H
#define LSST_QSERV_MASTER_SELECTLISTFACTORY_H
/**
  * @file SelectListFactory.h
  *
  * @brief SelectListFactory maintains parse state so that a SelectList can be
  * built from a parse tree. It populates some state for SelectFactory. 
  *
  * @author Daniel L. Wang, SLAC
  */

#include <list>
#include <map>
#include <antlr/AST.hpp>
#include <boost/shared_ptr.hpp>
#include <stdexcept>
namespace lsst { namespace qserv { namespace master {
// Forward
class ParseAliasMap;
class ValueExpr;
typedef boost::shared_ptr<ValueExpr> ValueExprPtr;
typedef std::list<ValueExprPtr> ValueExprList;

class SelectList;
class ValueExprFactory;

/// SelectListFactory is a factory for SelectLists from ANTLR nodes
class SelectListFactory {
public:
    boost::shared_ptr<SelectList> getProduct();
private:
    friend class SelectFactory;

    class SelectListH;
    class SelectStarH;
    friend class SelectStarH;
    class ColumnAliasH;
    class ParseException;

    // For "friends"
    SelectListFactory(boost::shared_ptr<ParseAliasMap> aliasMap,
                      boost::shared_ptr<ValueExprFactory> vf);
    void attachTo(SqlSQL2Parser& p);

    // Really private
    void _import(RefAST selectRoot);
    
    void _addSelectColumn(RefAST expr);
    void _addSelectStar(RefAST child=RefAST());
    ValueExprPtr _newColumnExpr(RefAST expr);
    ValueExprPtr _newSetFctSpec(RefAST expr);

    // Delegate handlers
    boost::shared_ptr<SelectListH> _selectListH;
    boost::shared_ptr<ColumnAliasH> _columnAliasH;

    // data
    boost::shared_ptr<ParseAliasMap> _aliases;
    boost::shared_ptr<ValueExprFactory> _vFactory;
    boost::shared_ptr<ValueExprList> _valueExprList;
    
};

class SelectListFactory::ParseException : public std::runtime_error {
public:
  explicit ParseException(RefAST subtree);
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_SELECTLISTFACTORY_H

