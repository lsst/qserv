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
/**
  * @file SelectFactory.cc
  *
  * @brief Implementation of the SelectFactory, which is responsible
  * (through some delegated behavior) for constructing SelectStmt (and
  * SelectList, etc.) from an ANTLR parse tree.
  *
  * Includes parse handlers: SelectListH, SelectStarH, ColumnAliasH
  *
  * @author Daniel L. Wang, SLAC
  */
#include "parser/SelectFactory.h"

// C++
#include <deque>
#include <iterator>

// Package
#include "SqlSQL2Parser.hpp" // applies several "using antlr::***".
#include "parser/ColumnRefH.h"

#include "query/SelectStmt.h"

#include "parser/SelectListFactory.h"
#include "query/SelectList.h"
#include "parser/FromFactory.h"
#include "parser/WhereFactory.h"
#include "parser/ModFactory.h"
#include "parser/ValueExprFactory.h"
#include "query/ValueFactor.h"

#include "parser/ParseAliasMap.h"
#include "parser/ParseException.h"
#include "parser/parseTreeUtil.h"
#include "query/TableRefN.h"

#include "log/Logger.h"

////////////////////////////////////////////////////////////////////////
// SelectFactory
////////////////////////////////////////////////////////////////////////
namespace lsst {
namespace qserv {
namespace parser {

SelectFactory::SelectFactory()
    : _columnAliases(new ParseAliasMap()),
      _tableAliases(new ParseAliasMap()),
      _columnRefNodeMap(new ColumnRefNodeMap()),
      _fFactory(new FromFactory(_tableAliases)),
      _vFactory(new ValueExprFactory(_columnRefNodeMap)) {

    _slFactory.reset(new SelectListFactory(_columnAliases, _vFactory));
    _mFactory.reset(new ModFactory(_vFactory));
    _wFactory.reset(new WhereFactory(_vFactory));
}

void
SelectFactory::attachTo(SqlSQL2Parser& p) {
    _attachShared(p);

    _slFactory->attachTo(p);
    _fFactory->attachTo(p);
    _wFactory->attachTo(p);
    _mFactory->attachTo(p);
}

boost::shared_ptr<query::SelectStmt>
SelectFactory::getStatement() {
    boost::shared_ptr<query::SelectStmt> stmt(new query::SelectStmt());
    stmt->_selectList = _slFactory->getProduct();
    stmt->_fromList = _fFactory->getProduct();
    stmt->_whereClause = _wFactory->getProduct();
    stmt->_orderBy = _mFactory->getOrderBy();
    stmt->_groupBy = _mFactory->getGroupBy();
    stmt->_having = _mFactory->getHaving();
    stmt->_limit = _mFactory->getLimit();
    return stmt;
}

void
SelectFactory::_attachShared(SqlSQL2Parser& p) {
    boost::shared_ptr<ColumnRefH> crh(new ColumnRefH());
    crh->setListener(_columnRefNodeMap);
    p._columnRefHandler = crh;
}

////////////////////////////////////////////////////////////////////////
// SelectListFactory::SelectListH
////////////////////////////////////////////////////////////////////////
class SelectListFactory::SelectListH : public VoidOneRefFunc {
public:
    explicit SelectListH(SelectListFactory& f) : _f(f) {}
    virtual ~SelectListH() {}
    virtual void operator()(RefAST a) {
        _f._import(a); // Trigger select list construction
    }
    SelectListFactory& _f;
};

////////////////////////////////////////////////////////////////////////
// SelectListFactory::SelectStarH
////////////////////////////////////////////////////////////////////////
class SelectListFactory::SelectStarH : public VoidOneRefFunc {
public:
    explicit SelectStarH(SelectListFactory& f) : _f(f) {}
    virtual ~SelectStarH() {}
    virtual void operator()(antlr::RefAST a) {
        _f._addSelectStar();
    }
private:
    SelectListFactory& _f;
}; // SelectStarH


////////////////////////////////////////////////////////////////////////
// SelectListFactory::ColumnAliasH
////////////////////////////////////////////////////////////////////////
class SelectListFactory::ColumnAliasH : public VoidTwoRefFunc {
public:
    ColumnAliasH(boost::shared_ptr<ParseAliasMap> map) : _map(map) {}
    virtual ~ColumnAliasH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
        if(b.get()) {
            b->setType(SqlSQL2TokenTypes::COLUMN_ALIAS_NAME);
            _map->addAlias(b, a);
        }
        // Save column ref for pass/fixup computation,
        // regardless of alias.
    }
private:
    boost::shared_ptr<ParseAliasMap> _map;
}; // class ColumnAliasH


////////////////////////////////////////////////////////////////////////
// class SelectListFactory
////////////////////////////////////////////////////////////////////////
SelectListFactory::SelectListFactory(boost::shared_ptr<ParseAliasMap> aliasMap,
                                     boost::shared_ptr<ValueExprFactory> vf)
    : _aliases(aliasMap),
      _vFactory(vf),
      _valueExprList(new ValueExprList()) {
}

void
SelectListFactory::attachTo(SqlSQL2Parser& p) {
    _selectListH.reset(new SelectListH(*this));
    _columnAliasH.reset(new ColumnAliasH(_aliases));
    p._selectListHandler = _selectListH;
    p._selectStarHandler.reset(new SelectStarH(*this));
    p._columnAliasHandler = _columnAliasH;
}

boost::shared_ptr<query::SelectList> 
SelectListFactory::getProduct() {
    boost::shared_ptr<query::SelectList> slist(new query::SelectList());
    slist->_valueExprList = _valueExprList;
    return slist;
}

void
SelectListFactory::_import(RefAST selectRoot) {
    //    LOGGER_INF << "Type of selectRoot is "
    //              << selectRoot->getType() << std::endl;

    for(; selectRoot.get();
        selectRoot = selectRoot->getNextSibling()) {
        RefAST child = selectRoot->getFirstChild();
        switch(selectRoot->getType()) {
        case SqlSQL2TokenTypes::SELECT_COLUMN:
            if(!child.get()) {
                throw ParseException("Expected select column", selectRoot);
            }
            _addSelectColumn(child);
            break;
        case SqlSQL2TokenTypes::SELECT_TABLESTAR:
            if(!child.get()) {
                throw ParseException("Missing table.*", selectRoot);
            }
            _addSelectStar(child);
            break;
        case SqlSQL2TokenTypes::ASTERISK: // Not sure this will be
                                          // handled here.
            _addSelectStar();
            // should only have a single unqualified "*"
            break;
        default:
            throw ParseException("Invalid SelectList token type",selectRoot);

        } // end switch
    } // end for-each select_list child.
}

void
SelectListFactory::_addSelectColumn(RefAST expr) {
    // Figure out what type of value expr, and create it properly.
    // LOGGER_INF << "SelectCol Type of:" << expr->getText()
    //           << "(" << expr->getType() << ")" << std::endl;
    if(!expr.get()) {
        throw std::invalid_argument("Attempted _addSelectColumn(NULL)");
    }
    if(expr->getType() != SqlSQL2TokenTypes::VALUE_EXP) {
        throw ParseException("Expected VALUE_EXP", expr);
    }
    RefAST child = expr->getFirstChild();
    if(!child.get()) {
        throw ParseException("Missing VALUE_EXP child", expr);
    }
    //    LOGGER_INF << "child is " << child->getType() << std::endl;
    ValueExprPtr ve = _vFactory->newExpr(child);

    // Annotate if alias found.
    RefAST alias = _aliases->getAlias(expr);
    if(alias.get()) {
        ve->setAlias(tokenText(alias));
    }
    _valueExprList->push_back(ve);
}

void
SelectListFactory::_addSelectStar(RefAST child) {
    // Note a "Select *".
    // If child.get(), this means that it's in the form of
    // "table.*". There might be sibling handling (i.e., multiple
    // table.* expressions).
    query::ValueFactorPtr vt;
    std::string tableName;
    if(child.get()) {
        // child should be QUALIFIED_NAME, so its child should be a
        // table name.
        RefAST table = child->getFirstChild();
        if(!table.get()) {
            throw ParseException("Missing name node.", child);
        }
        tableName = tokenText(table);
        LOGGER_INF << "table ref'd for *: " << tableName << std::endl;
    }
    vt = query::ValueFactor::newStarFactor(tableName);
    _valueExprList->push_back(query::ValueExpr::newSimple(vt));
}

}}} // namespace lsst::qserv::parser
