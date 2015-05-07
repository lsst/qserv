// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
  * @file
  *
  * @brief PredicateFactory makes Predicate objects.
  *
  * @author Daniel L. Wang, SLAC
  */

// Third-party headers
#include <antlr/AST.hpp>
#include <memory>

// Forward declarations
namespace lsst {
namespace qserv {
namespace parser {
    class ValueExprFactory;
}
namespace query {
    class BetweenPredicate;
    class CompPredicate;
    class InPredicate;
    class LikePredicate;
    class NullPredicate;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {

/// PredicateFactory is a factory for making Predicate objects
class PredicateFactory {
public:
    explicit PredicateFactory(ValueExprFactory& vf)
        : _vf(vf) {}
    std::shared_ptr<query::CompPredicate> newCompPredicate(antlr::RefAST a);
    std::shared_ptr<query::BetweenPredicate> newBetweenPredicate(antlr::RefAST a);
    std::shared_ptr<query::InPredicate> newInPredicate(antlr::RefAST a);
    std::shared_ptr<query::LikePredicate> newLikePredicate(antlr::RefAST a);
    std::shared_ptr<query::NullPredicate> newNullPredicate(antlr::RefAST a);
private:
    ValueExprFactory& _vf;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_PREDICATEFACTORY_H
