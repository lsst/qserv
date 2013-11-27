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
// PredicateFactory constructs Predicate instances from antlr nodes.

#ifndef LSST_QSERV_PARSER_PREDICATEFACTORY_H
#define LSST_QSERV_PARSER_PREDICATEFACTORY_H
/**
  * @file PredicateFactory.h
  *
  * @brief PredicateFactory makes Predicate objects.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <boost/shared_ptr.hpp>
#include <antlr/AST.hpp>

namespace lsst {
namespace qserv {

namespace query {
    // Forward
    class CompPredicate;
    class BetweenPredicate;
    class InPredicate;
    class LikePredicate;
} // namespace query


namespace parser {

// Forward
class ValueExprFactory;

/// PredicateFactory is a factory for making Predicate objects
class PredicateFactory {
public:
    explicit PredicateFactory(ValueExprFactory& vf)
        : _vf(vf) {}
    boost::shared_ptr<query::CompPredicate> newCompPredicate(antlr::RefAST a);
    boost::shared_ptr<query::BetweenPredicate> newBetweenPredicate(antlr::RefAST a);
    boost::shared_ptr<query::InPredicate> newInPredicate(antlr::RefAST a);
    boost::shared_ptr<query::LikePredicate> newLikePredicate(antlr::RefAST a);
private:
    ValueExprFactory& _vf;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_PREDICATEFACTORY_H
