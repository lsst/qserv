// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2019 AURA/LSST.
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


// Class header
#include "query/SecIdxRestrictor.h"

// System headers
#include <iostream>

// Third-party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "query/BetweenPredicate.h"
#include "query/CompPredicate.h"
#include "query/InPredicate.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"


namespace lsst {
namespace qserv {
namespace query {


std::ostream& operator<<(std::ostream& os, SecIdxRestrictor const& q) {
    QueryTemplate qt;
    q.renderTo(qt);
    os << qt;
    return os;
}


std::string SecIdxRestrictor::sqlFragment() const {
    QueryTemplate qt;
    renderTo(qt);
    return boost::lexical_cast<std::string>(qt);
}


bool SecIdxRestrictor::operator==(const SecIdxRestrictor& rhs) const {
    return typeid(*this) == typeid(rhs) && isEqual(rhs);
}


void SecIdxCompRestrictor::renderTo(QueryTemplate& qt) const {
    _compPredicate->renderTo(qt);
}


bool SecIdxCompRestrictor::isEqual(const SecIdxRestrictor& rhs) const {
    auto rhsCompRestrictor = static_cast<SecIdxCompRestrictor const&>(rhs);
    return *_compPredicate == *rhsCompRestrictor._compPredicate;
}


std::shared_ptr<query::ColumnRef const> SecIdxCompRestrictor::getSecIdxColumnRef() const {
    return _useLeft ? _compPredicate->left->getColumnRef() : _compPredicate->right->getColumnRef();
}


std::string SecIdxCompRestrictor::getSecIdxLookupQuery(std::string const& secondaryIndexDb,
        std::string const& secondaryIndexTable, std::string const& chunkColumn,
        std::string const& subChunkColumn) const {
    QueryTemplate columnRefQt;
    columnRefQt.setUseColumnOnly(true);
    _compPredicate->renderTo(columnRefQt);
    return "SELECT " + chunkColumn + ", " + subChunkColumn +
            " FROM " + secondaryIndexDb + "." + secondaryIndexTable +
            " WHERE " + boost::lexical_cast<std::string>(columnRefQt);
}


void SecIdxBetweenRestrictor::renderTo(QueryTemplate& qt) const {
    _betweenPredicate->renderTo(qt);
}


bool SecIdxBetweenRestrictor::isEqual(const SecIdxRestrictor& rhs) const {
    auto rhsBetweenRestrictor = static_cast<SecIdxBetweenRestrictor const&>(rhs);
    return *_betweenPredicate == *rhsBetweenRestrictor._betweenPredicate;
}


std::shared_ptr<query::ColumnRef const> SecIdxBetweenRestrictor::getSecIdxColumnRef() const {
    return _betweenPredicate->value->getColumnRef();
}


std::string SecIdxBetweenRestrictor::getSecIdxLookupQuery(std::string const& secondaryIndexDb,
        std::string const& secondaryIndexTable, std::string const& chunkColumn,
        std::string const& subChunkColumn) const {
    QueryTemplate columnRefQt;
    columnRefQt.setUseColumnOnly(true);
    _betweenPredicate->renderTo(columnRefQt);
    return "SELECT " + chunkColumn + ", " + subChunkColumn +
            " FROM " + secondaryIndexDb + "." + secondaryIndexTable +
            " WHERE " + boost::lexical_cast<std::string>(columnRefQt);
}


void SecIdxInRestrictor::renderTo(QueryTemplate& qt) const {
    _inPredicate->renderTo(qt);
}


bool SecIdxInRestrictor::isEqual(const SecIdxRestrictor& rhs) const {
    auto rhsRestrictor = static_cast<SecIdxInRestrictor const&>(rhs);
    return *_inPredicate == *rhsRestrictor._inPredicate;
}


std::shared_ptr<query::ColumnRef const> SecIdxInRestrictor::getSecIdxColumnRef() const {
    return _inPredicate->value->getColumnRef();
}


std::string SecIdxInRestrictor::getSecIdxLookupQuery(std::string const& secondaryIndexDb,
        std::string const& secondaryIndexTable, std::string const& chunkColumn,
        std::string const& subChunkColumn) const {
    QueryTemplate qt;
    qt.setUseColumnOnly(true);
    _inPredicate->renderTo(qt);
    return "SELECT " + chunkColumn + ", " + subChunkColumn +
            " FROM " + secondaryIndexDb + "." + secondaryIndexTable +
            " WHERE " + boost::lexical_cast<std::string>(qt);
}

}}} // namespace lsst::qserv::query
