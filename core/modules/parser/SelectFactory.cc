// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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
  * @brief Implementation of the SelectFactory, which is responsible
  * (through some delegated behavior) for constructing SelectStmt (and
  * SelectList, etc.) from an ANTLR parse tree.
  *
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "parser/SelectFactory.h"

// Third-party headers
#include "parser/SqlSQL2Parser.hpp" // applies several "using antlr::***".

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
   // parser: factories
#include "parser/FromFactory.h"
#include "parser/SelectListFactory.h"
#include "parser/WhereFactory.h"
#include "parser/ModFactory.h"
#include "parser/ValueExprFactory.h"
#include "parser/WhereFactory.h"
  // parser: all others
#include "parser/ColumnRefH.h"
#include "parser/ParseAliasMap.h"
#include "parser/ParseException.h"
#include "parser/parseTreeUtil.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueFactor.h"


////////////////////////////////////////////////////////////////////////
// SelectFactory
////////////////////////////////////////////////////////////////////////
namespace lsst {
namespace qserv {
namespace parser {

SelectFactory::SelectFactory()
    : _columnAliases(std::make_shared<ParseAliasMap>()),
      _tableAliases(std::make_shared<ParseAliasMap>()),
      _columnRefNodeMap(std::make_shared<ColumnRefNodeMap>()),
      _hasDistinct(false),
      _vFactory(std::make_shared<ValueExprFactory>(_columnRefNodeMap)) {

    _fFactory = std::make_shared<FromFactory>(_tableAliases, _vFactory);
    _slFactory = std::shared_ptr<SelectListFactory>(
            new SelectListFactory(_columnAliases, _vFactory));
    _mFactory = std::make_shared<ModFactory>(_vFactory);
    _wFactory = std::make_shared<WhereFactory>(_vFactory);
}

void
SelectFactory::attachTo(SqlSQL2Parser& p) {
    _attachShared(p);

    _slFactory->attachTo(p);
    _fFactory->attachTo(p);
    _wFactory->attachTo(p);
    _mFactory->attachTo(p);
}

std::shared_ptr<query::SelectStmt>
SelectFactory::getStatement() {
    std::shared_ptr<query::SelectStmt> stmt = std::make_shared<query::SelectStmt>();
    stmt->_selectList = _slFactory->getProduct();
    stmt->_fromList = _fFactory->getProduct();
    stmt->_whereClause = _wFactory->getProduct();
    stmt->_orderBy = _mFactory->getOrderBy();
    stmt->_groupBy = _mFactory->getGroupBy();
    stmt->_having = _mFactory->getHaving();
    stmt->_limit = _mFactory->getLimit();
    stmt->_hasDistinct = _hasDistinct;
    return stmt;
}

/// Handle:
/// query_spec :
///        "select" (set_quantifier)? select_list (into_clause)? table_exp  {
///                #query_spec =  #([QUERY_SPEC,"QUERY_SPEC"], #query_spec);
class QuerySpecH : public VoidOneRefFunc {
public:
    QuerySpecH(SelectFactory& sf, SelectListFactory& slf)
        : _sf(sf), _slf(slf) {}
    virtual ~QuerySpecH() {}
    virtual void operator()(RefAST a) {
        RefAST selectRoot = a;
        //std::cout << "query spec got " << walkIndentedString(a) << std::endl;
        //a; // should point at "SELECT"
        a = a->getNextSibling();
        RefAST child;
        for(; a; a = a->getNextSibling()) {
            switch(a->getType()) {
              case SqlSQL2TokenTypes::SQL2RW_distinct:
                  _sf.setDistinct(true);
                  break;
              case SqlSQL2TokenTypes::SELECT_LIST:
                  child = a->getFirstChild();

                  if(!child.get()) {
                      throw ParseException("Expected select list", a);
                  } else {
                      _slf.import(child);
                  }
                  break;
              case SqlSQL2TokenTypes::ASTERISK:
                  _slf.importStar(a);
                  break;
              case SqlSQL2TokenTypes::FROM_CLAUSE:
              case SqlSQL2TokenTypes::WHERE_CLAUSE:
                  // For now, defer FROM and WHERE handling to parse handlers.
                  // Good place to call those handlers in the future.
                  break;
              default:
                  //std::cout << "Unhandled queryspec node:"
                  //          << tokenText(a) << std::endl;
                  // For now, ignore into_clause and table_exp
                  // and let the other parse handlers take it.
                  break;
            }
        }
    }
    SelectFactory& _sf;
    SelectListFactory& _slf;
};

void
SelectFactory::_attachShared(SqlSQL2Parser& p) {
    std::shared_ptr<ColumnRefH> crh = std::make_shared<ColumnRefH>();
    // Non-const argument, can't use make_shared.
    std::shared_ptr<QuerySpecH> qsh(new QuerySpecH(*this, *_slFactory));
    crh->setListener(_columnRefNodeMap);
    p._columnRefHandler = crh;
    p._querySpecHandler = qsh;
}

}}} // namespace lsst::qserv::parser
