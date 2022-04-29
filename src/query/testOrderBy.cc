// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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

// list must be included before boost/test/data/test_case.hpp, because it is used there but not included.
// (or that file could be included after boost/test/unit_test.hpp, which does cause list to be
// included. But, we like to include our headers alphabetically so I'm including list here.
#include <list>
#include <stdexcept>
#include <string>

// Qserv headers
#include "query/FuncExpr.h"
#include "query/OrderByClause.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"

// Boost unit test header
#define BOOST_TEST_MODULE OrderBy
#include "boost/test/unit_test.hpp"

using namespace lsst::qserv::query;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(clone) {
    auto valueExpr = std::make_shared<ValueExpr>();
    valueExpr->addValueFactor(ValueFactor::newAggFactor(FuncExpr::newArg1("MAX", "raFlux")));
    valueExpr->addOp(ValueExpr::MINUS);
    valueExpr->addValueFactor(ValueFactor::newAggFactor(FuncExpr::newArg1("MIN", "raFlux")));
    valueExpr->setAlias("flx");
    auto orderByVec = std::make_shared<OrderByClause::OrderByTermVector>();
    orderByVec->push_back(OrderByTerm(valueExpr));
    OrderByClause orderBy(orderByVec);
    auto clonedOrderBy = orderBy.clone();
    BOOST_REQUIRE_EQUAL(clonedOrderBy->getTerms()->size(), 1u);
    // verify the first term of each clause is not the same object
    BOOST_CHECK(&clonedOrderBy->getTerms()->at(0) != &orderBy.getTerms()->at(0));
    // verify the value expr in the first term of each clause is not the same object
    BOOST_CHECK(clonedOrderBy->getTerms()->at(0).getExpr() != orderBy.getTerms()->at(0).getExpr());
}

BOOST_AUTO_TEST_SUITE_END()
