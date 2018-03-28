// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
  * @brief Implementation of the SelectFactory, which is responsible
  * (through some delegated behavior) for constructing SelectStmt (and
  * SelectList, etc.) from an ANTLR parse tree.
  *
  * Includes parse handlers: SelectStarH, ColumnAliasH
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "parser/SelectListFactory.h"

// Third-party headers

// Qserv headers
#include "parser/ParseAliasMap.h"
#include "parser/ParseException.h"
#include "parser/parseTreeUtil.h"
#include "parser/SqlSQL2Parser.hpp" // applies several "using antlr::***".
#include "parser/ValueExprFactory.h"
#include "query/SelectList.h"
#include "query/ValueFactor.h"


namespace lsst {
namespace qserv {
namespace parser {

void SelectListFactory::addValueExpr(std::shared_ptr<query::SelectList> selectList,
                                     ValueExprPtr valueExpr) {
    selectList->_valueExprList->push_back(valueExpr);
}


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
    ColumnAliasH(std::shared_ptr<ParseAliasMap> map) : _map(map) {}
    virtual ~ColumnAliasH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
        if (b.get()) {
            b->setType(SqlSQL2TokenTypes::COLUMN_ALIAS_NAME);
            _map->addAlias(b, a);
        }
        // Save column ref for pass/fixup computation,
        // regardless of alias.
    }
private:
    std::shared_ptr<ParseAliasMap> _map;
}; // class ColumnAliasH


////////////////////////////////////////////////////////////////////////
// class SelectListFactory
////////////////////////////////////////////////////////////////////////
SelectListFactory::SelectListFactory(std::shared_ptr<ValueExprFactory> vf)
    : _columnAliases(std::make_shared<ParseAliasMap>()),
      _vFactory(vf),
      _valueExprList(std::make_shared<ValueExprPtrVector>()) {
}

/// attach the column alias handler. This is needed until we implement code to
/// visit the tree to handle column aliases. We can probably put the visit code
/// in a function to be called at the beginning of the import() call.
void
SelectListFactory::attachTo(SqlSQL2Parser& p) {
    _columnAliasH = std::make_shared<ColumnAliasH>(_columnAliases);
    p._columnAliasHandler = _columnAliasH;
}

std::shared_ptr<query::SelectList> SelectListFactory::getProduct() {
    std::shared_ptr<query::SelectList> slist = std::make_shared<query::SelectList>();
    slist->_valueExprList = _valueExprList;
    return slist;
}

void
SelectListFactory::import(RefAST selectRoot) {
    for(; selectRoot.get();
        selectRoot = selectRoot->getNextSibling()) {
        RefAST child = selectRoot->getFirstChild();
        switch(selectRoot->getType()) {
        case SqlSQL2TokenTypes::SELECT_COLUMN:
            if (!child.get()) {
                throw ParseException("Expected select column", selectRoot);
            }
            _addSelectColumn(child);
            break;
        case SqlSQL2TokenTypes::SELECT_TABLESTAR:
            if (!child.get()) {
                throw ParseException("Missing table.*", selectRoot);
            }
            _addSelectStar(child);
            break;
        case SqlSQL2TokenTypes::ASTERISK: // Not sure this will be
                                          // handled here.
            throw ParseException("Unexpected * in SELECT_LIST", selectRoot);
            break;
        default:
            throw ParseException("Invalid SelectList token type", selectRoot);

        } // end switch
    } // end for-each select_list child.
}

void
SelectListFactory::importStar(RefAST selectRoot) {
    _addSelectStar();
}

void
SelectListFactory::addSelectAggFunction(std::shared_ptr<query::SelectList>& selectList,
                                        std::shared_ptr<query::ValueExpr>& func) {
    if (nullptr == selectList) {
        throw std::runtime_error("null selectList ptr");
    }
    selectList->_valueExprList->push_back(func);
}

void
SelectListFactory::_addSelectColumn(RefAST expr) {
    // Figure out what type of value expr, and create it properly.
    // std::cout << "SelectCol Type of:" << expr->getText()
    //           << "(" << expr->getType() << ")" << std::endl;
    if (!expr.get()) {
        throw std::invalid_argument("Attempted _addSelectColumn(NULL)");
    }
    if (expr->getType() != SqlSQL2TokenTypes::VALUE_EXP) {
        throw ParseException("Expected VALUE_EXP", expr);
    }
    RefAST child = expr->getFirstChild();
    if (!child.get()) {
        throw ParseException("Missing VALUE_EXP child", expr);
    }
    //    std::cout << "child is " << child->getType() << std::endl;
    ValueExprPtr ve = _vFactory->newExpr(child);

    // Annotate if alias found.
    RefAST alias = _columnAliases->getAlias(expr);
    if (alias.get()) {
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
    if (child.get()) {
        // child should be QUALIFIED_NAME, so its child should be a
        // table name.
        RefAST table = child->getFirstChild();
        if (!table.get()) {
            throw ParseException("Missing name node.", child);
        }
        tableName = tokenText(table);
    }
    vt = query::ValueFactor::newStarFactor(tableName);
    _valueExprList->push_back(query::ValueExpr::newSimple(vt));
}

}}} // Namespace lsst::qserv::parser
