// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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
  * @file
  *
  * @brief Implementation of ModFactory, which is responsible for
  * constructing representations of LIMIT, ORDER BY, and GROUP BY
  * clauses. It has a placeholder-grade support for HAVING.
  * Nested handlers: LimitH, OrderByH, GroupByH, HavingH
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "parser/ModFactory.h"

// System headers
#include <iterator>

// Third-party headers
#include "parser/SqlSQL2Parser.hpp" // applies several "using antlr::***".

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "parser/BoolTermFactory.h"
#include "parser/parserBase.h"   // Handler base classes
#include "parser/parseTreeUtil.h"
#include "parser/ParseException.h"
#include "parser/ValueExprFactory.h"
#include "query/GroupByClause.h" // Clauses
#include "query/HavingClause.h"  // Clauses
#include "query/OrderByClause.h" // Clauses

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.parser.ModFactory");
}

namespace lsst {
namespace qserv {
namespace parser {

////////////////////////////////////////////////////////////////////////
// ModFactory::LimitH
////////////////////////////////////////////////////////////////////////
class ModFactory::LimitH : public VoidOneRefFunc {
public:
    LimitH(ModFactory& mf) : _mf(mf) {}
    virtual void operator()(antlr::RefAST n) {
        _mf._importLimit(n);
    }
private:
    ModFactory& _mf;
};
////////////////////////////////////////////////////////////////////////
// ModFactory::OrderByH
////////////////////////////////////////////////////////////////////////
class ModFactory::OrderByH : public VoidOneRefFunc {
public:
    OrderByH(ModFactory& mf) : _mf(mf) {}
    virtual void operator()(antlr::RefAST n) {
        _mf._importOrderBy(n);
    }
private:
    ModFactory& _mf;
};
////////////////////////////////////////////////////////////////////////
// ModFactory::GroupByH
////////////////////////////////////////////////////////////////////////
class ModFactory::GroupByH : public VoidOneRefFunc {
public:
    GroupByH(ModFactory& mf) : _mf(mf) {}
    virtual void operator()(antlr::RefAST n) {
        _mf._importGroupBy(n);
    }
private:
    ModFactory& _mf;
};
////////////////////////////////////////////////////////////////////////
// ModFactory::HavingH
////////////////////////////////////////////////////////////////////////
class ModFactory::HavingH : public VoidOneRefFunc {
public:
    HavingH(ModFactory& mf) : _mf(mf) {}
    virtual void operator()(antlr::RefAST n) {
        _mf._importHaving(n);
    }
private:
    ModFactory& _mf;
};
////////////////////////////////////////////////////////////////////////
// ModFactory
////////////////////////////////////////////////////////////////////////
ModFactory::ModFactory(std::shared_ptr<ValueExprFactory> vf)
    : _vFactory(vf),
      _limit(NOTSET)
{
    if (!vf) {
        throw std::invalid_argument("ModFactory requires ValueExprFactory");
    }
}

void ModFactory::attachTo(SqlSQL2Parser& p) {
    p._limitHandler.reset(new LimitH(*this));
    p._orderByHandler.reset(new OrderByH(*this));
    p._groupByHandler.reset(new GroupByH(*this));
    p._havingHandler.reset(new HavingH(*this));
}

void ModFactory::_importLimit(antlr::RefAST a) {
    // Limit always has an int.
    LOGS(_log, LOG_LVL_DEBUG, "Limit got " << walkTreeString(a));
    if (!a.get()) {
        throw std::invalid_argument("Cannot _importLimit(NULL)");
    }
    std::stringstream ss(a->getText());
    ss >> _limit;
}

void ModFactory::_importOrderBy(antlr::RefAST a) {
    if (!a.get()) {
        throw std::invalid_argument("Cannot _importOrderBy(NULL)");
    }
    _orderBy = std::make_shared<query::OrderByClause>();
    // ORDER BY takes a column ref (expression)
    LOGS(_log, LOG_LVL_DEBUG, "ORDER BY got " << walkTreeString(a));
    while(a.get()) {
        if (a->getType() == SqlSQL2TokenTypes::COMMA) {
            a = a->getNextSibling();
            continue;
        }
        if (a->getType() != SqlSQL2TokenTypes::SORT_SPEC) {
            LOGS(_log, LOG_LVL_ERROR, "ORDER BY expected sort spec and got " << a->getText());
            throw std::logic_error("Expected SORT_SPEC token)");
        }
        RefAST key = a->getFirstChild();
        query::OrderByTerm ob;
        ob._order = query::OrderByTerm::DEFAULT;
        std::shared_ptr<query::ValueExpr> ve;
        if (key->getType() == SqlSQL2TokenTypes::SORT_KEY) {
            ob._expr = _vFactory->newExpr(key->getFirstChild());
            RefAST sib = key->getNextSibling();
            if (sib.get()
               && (sib->getType() == SqlSQL2TokenTypes::COLLATE_CLAUSE)) {
                RefAST cc = sib->getFirstChild();
                ob._collate = walkTreeString(cc);
                //orderby.addCollateClause(cc);
                sib = sib->getNextSibling();
            }
            if (sib.get()) {
                switch(sib->getType()) {
                case SqlSQL2TokenTypes::SQL2RW_asc:
                    ob._order = query::OrderByTerm::ASC;
                    break;
                case SqlSQL2TokenTypes::SQL2RW_desc:
                    ob._order = query::OrderByTerm::DESC;
                    break;
                default:
                    throw ParseException("unknown order-by syntax", a);
                    break;
                }
            }
            _orderBy->_addTerm(ob);
        } else if (key->getType() == SqlSQL2TokenTypes::UNSIGNED_INTEGER) {
            throw ParseException("positional order-by not allowed", a);
        } else {
            throw ParseException("unknown order-by syntax", a);
        }
        a = a->getNextSibling();
    }
    LOGS(_log, LOG_LVL_DEBUG, "ORDER BY AST branch processed: " << *_orderBy);
}

void ModFactory::_importGroupBy(antlr::RefAST a) {
    _groupBy = std::make_shared<query::GroupByClause>();
    // GROUP BY takes a column reference (expression?)
    if (!a.get()) {
        throw std::invalid_argument("Cannot _importGroupBy(NULL)");
    }
    while(a.get()) {
        if ((a->getType() != SqlSQL2TokenTypes::GROUPING_COLUMN_REF)) {
            throw std::logic_error("Attempting _import of non-grouping column");
        }
        query::GroupByTerm gb;
        RefAST key = a->getFirstChild();
        std::shared_ptr<query::ValueExpr> ve;
        if (key->getType() == SqlSQL2TokenTypes::COLUMN_REF) {
            gb._expr = _vFactory->newExpr(key->getFirstChild());
            RefAST sib = key->getNextSibling();
            if (sib.get()
               && (sib->getType() == SqlSQL2TokenTypes::COLLATE_CLAUSE)) {
                RefAST cc = sib->getFirstChild();
                gb._collate = walkTreeString(cc);
                sib = sib->getNextSibling();
            }
        } else {
            throw ParseException("group-by import error", a);
        }
        _groupBy->_addTerm(gb);
        a = a->getNextSibling();
    }

}

void ModFactory::_importHaving(antlr::RefAST a) {
    _having = std::make_shared<query::HavingClause>();
    // HAVING takes an boolean expression that is dependent on an
    // aggregation expression that was specified in the select list.
    // Online examples for SQL HAVING always have only one aggregation
    // and only one boolean, so this code will only accept single
    // aggregation, single boolean expressions.
    // e.g. HAVING count(obj.ra_PS_sigma) > 0.04
    if (!a.get()) {
        throw std::invalid_argument("Cannot _importHaving(NULL)");
    }
    // For now, we will silently traverse and recognize but ignore.

    // TODO:
    // Find boolean statement. Use it.
    // Record a failure if multiple boolean statements are found.
    // Render the bool terms just like WhereClause bool terms.
    if (a.get() && (a->getType() == SqlSQL2TokenTypes::OR_OP)) {
        antlr::RefAST first = a->getFirstChild();
        if (first.get()
           && (first->getType() == SqlSQL2TokenTypes::AND_OP)) {
            antlr::RefAST second = first->getFirstChild();
            if (second.get()) {
                BoolTermFactory f(_vFactory);
                _having->_tree = f.newBoolTerm(a);
                return;
            }
        }
    }
    _having->_tree.reset(); // NULL-out. Unhandled syntax.

    LOGS(_log, LOG_LVL_WARN, "Parse warning: HAVING clause unhandled.");
}

}}} // namespace lsst::qserv::parser
