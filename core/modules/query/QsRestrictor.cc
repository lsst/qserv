// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 AURA/LSST.
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
  * @author Daniel L. Wang, SLAC
  */


// Class header
#include "query/QsRestrictor.h"

// System headers
#include <iostream>
#include <iterator>

// Qserv headers
#include "qproc/geomAdapter.h"
#include "query/BetweenPredicate.h"
#include "query/BoolFactor.h"
#include "query/CompPredicate.h"
#include "query/FuncExpr.h"
#include "query/InPredicate.h"
#include "query/QueryTemplate.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "util/IterableFormatter.h"

namespace {

template <typename T>
std::vector<T> convertVec(std::vector<std::string> const& v) {
    std::vector<T> out;
    out.reserve(v.size());
    // todo add try/catch bad_lexical_cast here & rethrow the correct error type
    std::transform(v.begin(), v.end(), std::back_inserter(out), boost::lexical_cast<T, std::string>);
    return out;
}

}


namespace lsst {
namespace qserv {
namespace query {


std::ostream& operator<<(std::ostream& os, AreaRestrictor const& q) {
    return q.dbgPrint(os);
}


bool AreaRestrictor::operator==(const AreaRestrictor& rhs) const {
    return typeid(*this) == typeid(rhs) && isEqual(rhs);
}


AreaRestrictorBox::AreaRestrictorBox(std::string const& lonMinDegree, std::string const& latMinDegree,
        std::string const& lonMaxDegree, std::string const& latMaxDegree)
        : _lonMinDegree(lonMinDegree), _latMinDegree(latMinDegree), _lonMaxDegree(lonMaxDegree),
          _latMaxDegree(latMaxDegree)
{}


AreaRestrictorBox::AreaRestrictorBox(std::vector<std::string> const& parameters) {
    if (parameters.size() != 4) {
        throw std::logic_error("AreaRestrictorBox requires 4 parameters.");
    }
    _lonMinDegree = parameters[0];
    _latMinDegree = parameters[1];
    _lonMaxDegree = parameters[2];
    _latMaxDegree = parameters[3];
}


void AreaRestrictorBox::renderTo(QueryTemplate& qt) const {
    qt.append("qserv_areaspec_box");
    qt.append("(");
    qt.append(_lonMinDegree);
    qt.append(",");
    qt.append(_latMinDegree);
    qt.append(",");
    qt.append(_lonMaxDegree);
    qt.append(",");
    qt.append(_latMaxDegree);
    qt.append(")");
}


std::ostream& AreaRestrictorBox::dbgPrint(std::ostream& os) const {
    QueryTemplate qt;
    renderTo(qt);
    os << qt;
    return os;
}


bool AreaRestrictorBox::isEqual(const AreaRestrictor& rhs) const {
    auto rhsBox = static_cast<AreaRestrictorBox const&>(rhs);
    return (_lonMinDegree == rhsBox._lonMinDegree &&
            _latMinDegree == rhsBox._latMinDegree &&
            _lonMaxDegree == rhsBox._lonMaxDegree &&
            _latMaxDegree == rhsBox._latMaxDegree);
}


std::shared_ptr<query::BoolFactor> AreaRestrictorBox::asSciSqlFactor(std::string const& tableAlias,
        std::pair<std::string, std::string> const& chunkColumns) const {
    std::vector<std::shared_ptr<query::ValueExpr>> parameters = {
        query::ValueExpr::newColumnExpr("","", tableAlias, chunkColumns.first),
        query::ValueExpr::newColumnExpr("","", tableAlias, chunkColumns.second),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_lonMinDegree)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_latMinDegree)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_lonMaxDegree)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_latMaxDegree))
    };
    auto func = std::make_shared<query::FuncExpr>("scisql_s2PtInBox", parameters);
    auto compPred = std::make_shared<query::CompPredicate>(
        query::ValueExpr::newSimple(query::ValueFactor::newFuncFactor(func)),
        query::CompPredicate::EQUALS_OP,
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor("1")));
    return std::make_shared<query::BoolFactor>(compPred);
}


std::shared_ptr<sphgeom::Region> AreaRestrictorBox::getRegion() const {

    return qproc::getBoxFromParams(convertVec<double>({_lonMinDegree, _latMinDegree,
                                                       _lonMaxDegree, _latMaxDegree}));
}


AreaRestrictorCircle::AreaRestrictorCircle(std::string const& centerLonDegree,
        std::string const& centerLatDegree, std::string const& radiusDegree)
        : _centerLonDegree(centerLonDegree), _centerLatDegree(centerLatDegree), _radiusDegree(radiusDegree)
{}


AreaRestrictorCircle::AreaRestrictorCircle(std::vector<std::string> const& parameters) {
    if (parameters.size() != 3) {
        throw std::logic_error("qserv_areaspec_circle requires 3 parameters.");
    }
    _centerLonDegree = parameters[0];
    _centerLatDegree = parameters[1];
    _radiusDegree = parameters[2];
}


void AreaRestrictorCircle::renderTo(QueryTemplate& qt) const {
    qt.append("qserv_areaspec_circle");
    qt.append("(");
    qt.append(_centerLonDegree);
    qt.append(",");
    qt.append(_centerLatDegree);
    qt.append(",");
    qt.append(_radiusDegree);
    qt.append(")");
}


std::ostream& AreaRestrictorCircle::dbgPrint(std::ostream& os) const {
    QueryTemplate qt;
    renderTo(qt);
    os << qt;
    return os;
}


bool AreaRestrictorCircle::isEqual(const AreaRestrictor& rhs) const {
    auto rhsCircle = static_cast<AreaRestrictorCircle const&>(rhs);
    return (_centerLatDegree == rhsCircle._centerLatDegree &&
            _centerLonDegree == rhsCircle._centerLonDegree &&
            _radiusDegree == rhsCircle._radiusDegree);
}


std::shared_ptr<query::BoolFactor> AreaRestrictorCircle::asSciSqlFactor(std::string const& tableAlias,
        std::pair<std::string, std::string> const& chunkColumns) const {
    std::vector<std::shared_ptr<query::ValueExpr>> parameters = {
        query::ValueExpr::newColumnExpr("","", tableAlias, chunkColumns.first),
        query::ValueExpr::newColumnExpr("","", tableAlias, chunkColumns.second),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_centerLonDegree)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_centerLatDegree)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_radiusDegree)),
    };
    auto func = std::make_shared<query::FuncExpr>("scisql_s2PtInCircle", parameters);
    auto compPred = std::make_shared<query::CompPredicate>(
        query::ValueExpr::newSimple(query::ValueFactor::newFuncFactor(func)),
        query::CompPredicate::EQUALS_OP,
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor("1")));
    return std::make_shared<query::BoolFactor>(compPred);
}


std::shared_ptr<sphgeom::Region> AreaRestrictorCircle::getRegion() const {

    return qproc::getCircleFromParams(convertVec<double>({_centerLonDegree, _centerLatDegree,
                                                          _radiusDegree}));
}


AreaRestrictorEllipse::AreaRestrictorEllipse(std::string const& centerLonDegree,
        std::string const& centerLatDegree, std::string const& semiMajorAxisAngleArcsec,
        std::string const& semiMinorAxisAngleArcsec, std::string const& positionAngleDegree)
        : _centerLonDegree(centerLonDegree),
        _centerLatDegree(centerLatDegree),
        _semiMajorAxisAngleArcsec(semiMajorAxisAngleArcsec),
        _semiMinorAxisAngleArcsec(semiMinorAxisAngleArcsec),
        _positionAngleDegree(positionAngleDegree)
{}


AreaRestrictorEllipse::AreaRestrictorEllipse(std::vector<std::string> const& parameters) {
    if (parameters.size() != 5) {
        throw std::logic_error("qserv_areaspec_ellipse requires 5 parameters.");
    }
    _centerLonDegree = parameters[0];
    _centerLatDegree = parameters[1];
    _semiMajorAxisAngleArcsec = parameters[2];
    _semiMinorAxisAngleArcsec = parameters[3];
    _positionAngleDegree = parameters[4];
}


void AreaRestrictorEllipse::renderTo(QueryTemplate& qt) const {
    qt.append("qserv_areaspec_ellipse");
    qt.append("(");
    qt.append(_centerLonDegree);
    qt.append(",");
    qt.append(_centerLatDegree);
    qt.append(",");
    qt.append(_semiMajorAxisAngleArcsec);
    qt.append(",");
    qt.append(_semiMinorAxisAngleArcsec);
    qt.append(",");
    qt.append(_positionAngleDegree);
    qt.append(")");
}


std::ostream& AreaRestrictorEllipse::dbgPrint(std::ostream& os) const {
    QueryTemplate qt;
    renderTo(qt);
    os << qt;
    return os;
}


bool AreaRestrictorEllipse::isEqual(const AreaRestrictor& rhs) const {
    auto rhsEllipse = static_cast<AreaRestrictorEllipse const&>(rhs);
    return (_centerLonDegree == rhsEllipse._centerLonDegree &&
            _centerLatDegree == rhsEllipse._centerLatDegree &&
            _semiMajorAxisAngleArcsec == rhsEllipse._semiMajorAxisAngleArcsec &&
            _semiMinorAxisAngleArcsec == rhsEllipse._semiMinorAxisAngleArcsec &&
            _positionAngleDegree == rhsEllipse._positionAngleDegree);
}


std::shared_ptr<query::BoolFactor> AreaRestrictorEllipse::asSciSqlFactor(std::string const& tableAlias,
        std::pair<std::string, std::string> const& chunkColumns) const {
    std::vector<std::shared_ptr<query::ValueExpr>> parameters = {
        query::ValueExpr::newColumnExpr("","", tableAlias, chunkColumns.first),
        query::ValueExpr::newColumnExpr("","", tableAlias, chunkColumns.second),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_centerLonDegree)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_centerLatDegree)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_semiMajorAxisAngleArcsec)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_semiMinorAxisAngleArcsec)),
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(_positionAngleDegree)),
    };
    auto func = std::make_shared<query::FuncExpr>("scisql_s2PtInEllipse", parameters);
    auto compPred = std::make_shared<query::CompPredicate>(
        query::ValueExpr::newSimple(query::ValueFactor::newFuncFactor(func)),
        query::CompPredicate::EQUALS_OP,
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor("1")));
    return std::make_shared<query::BoolFactor>(compPred);
}


std::shared_ptr<sphgeom::Region> AreaRestrictorEllipse::getRegion() const {

    return qproc::getEllipseFromParams(convertVec<double>({_centerLonDegree, _centerLatDegree,
        _semiMajorAxisAngleArcsec, _semiMinorAxisAngleArcsec, _positionAngleDegree}));
}


AreaRestrictorPoly::AreaRestrictorPoly(std::vector<std::string> const& parameters)
        : _parameters(parameters) {
    if (_parameters.size() % 2 != 0) {
        throw std::logic_error("AreaRestrictorPoly requires an even number of arguments.");
    }
}


void AreaRestrictorPoly::renderTo(QueryTemplate& qt) const {
    qt.append("qserv_areaspec_poly");
    qt.append("(");
    bool first = true;
    for (auto const& parameter : _parameters) {
        if (first) {
            first = false;
        } else {
            qt.append(",");
        }
        qt.append(parameter);
    }
    qt.append(")");
}


std::ostream& AreaRestrictorPoly::dbgPrint(std::ostream& os) const {
    QueryTemplate qt;
    renderTo(qt);
    os << qt;
    return os;
}


bool AreaRestrictorPoly::isEqual(const AreaRestrictor& rhs) const {
    auto rhsPoly = static_cast<AreaRestrictorPoly const&>(rhs);
    return (_parameters.size() == rhsPoly._parameters.size() &&
            std::equal(_parameters.begin(), _parameters.end(), rhsPoly._parameters.begin()));
}


std::shared_ptr<query::BoolFactor> AreaRestrictorPoly::asSciSqlFactor(std::string const& tableAlias,
        std::pair<std::string, std::string> const& chunkColumns) const {
    std::vector<std::shared_ptr<query::ValueExpr>> parameters = {
        query::ValueExpr::newColumnExpr("","", tableAlias, chunkColumns.first),
        query::ValueExpr::newColumnExpr("","", tableAlias, chunkColumns.second),};
    std::transform(_parameters.begin(), _parameters.end(), std::back_inserter(parameters),
        [] (std::string const& parameter) -> std::shared_ptr<query::ValueExpr> {
            return query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(parameter));
        });
    auto func = std::make_shared<query::FuncExpr>("scisql_s2PtInCPoly", parameters);
    auto compPred = std::make_shared<query::CompPredicate>(
        query::ValueExpr::newSimple(query::ValueFactor::newFuncFactor(func)),
        query::CompPredicate::EQUALS_OP,
        query::ValueExpr::newSimple(query::ValueFactor::newConstFactor("1")));
    return std::make_shared<query::BoolFactor>(compPred);
}


std::shared_ptr<sphgeom::Region> AreaRestrictorPoly::getRegion() const {

    return qproc::getConvexPolyFromParams(convertVec<double>(_parameters));
}


std::ostream& operator<<(std::ostream& os, SIRestrictor const& q) {
    return q.dbgPrint(os);
}


bool SIRestrictor::operator==(const SIRestrictor& rhs) const {
    return typeid(*this) == typeid(rhs) && isEqual(rhs);
}


void SICompRestrictor::renderTo(QueryTemplate& qt) const {
    _compPredicate->renderTo(qt);
}


bool SICompRestrictor::isEqual(const SIRestrictor& rhs) const {
    auto rhsCompRestrictor = static_cast<SICompRestrictor const&>(rhs);
    return *_compPredicate == *rhsCompRestrictor._compPredicate;
}


std::ostream& SICompRestrictor::dbgPrint(std::ostream& os) const {
    os << "SICompRestrictor(" << *_compPredicate << ")";
    return os;
}


std::shared_ptr<query::ColumnRef const> SICompRestrictor::getSecondaryIndexColumnRef() const {
    return _useLeft ? _compPredicate->left->getColumnRef() : _compPredicate->right->getColumnRef();
}


std::string SICompRestrictor::getSILookupQuery(std::string const& secondaryIndexDb,
        std::string const& secondaryIndexTable, std::string const& chunkColumn,
        std::string const& subChunkColumn) const {
    QueryTemplate columnRefQt;
    columnRefQt.setUseColumnOnly(true);
    _compPredicate->renderTo(columnRefQt);
    return "SELECT " + chunkColumn + ", " + subChunkColumn +
            " FROM " + secondaryIndexDb + "." + secondaryIndexTable +
            " WHERE " + boost::lexical_cast<std::string>(columnRefQt);
}


void SIBetweenRestrictor::renderTo(QueryTemplate& qt) const {
    _betweenPredicate->renderTo(qt);
}


bool SIBetweenRestrictor::isEqual(const SIRestrictor& rhs) const {
    auto rhsBetweenRestrictor = static_cast<SIBetweenRestrictor const&>(rhs);
    return *_betweenPredicate == *rhsBetweenRestrictor._betweenPredicate;
}


std::ostream& SIBetweenRestrictor::dbgPrint(std::ostream& os) const {
    os << "SIBetweenRestrictor(" << *_betweenPredicate << ")";
    return os;
}


std::shared_ptr<query::ColumnRef const> SIBetweenRestrictor::getSecondaryIndexColumnRef() const {
    return _betweenPredicate->value->getColumnRef();
}


std::string SIBetweenRestrictor::getSILookupQuery(std::string const& secondaryIndexDb,
        std::string const& secondaryIndexTable, std::string const& chunkColumn,
        std::string const& subChunkColumn) const {
    QueryTemplate columnRefQt;
    columnRefQt.setUseColumnOnly(true);
    _betweenPredicate->renderTo(columnRefQt);
    return "SELECT " + chunkColumn + ", " + subChunkColumn +
            " FROM " + secondaryIndexDb + "." + secondaryIndexTable +
            " WHERE " + boost::lexical_cast<std::string>(columnRefQt);
}


void SIInRestrictor::renderTo(QueryTemplate& qt) const {
    _inPredicate->renderTo(qt);
}


bool SIInRestrictor::isEqual(const SIRestrictor& rhs) const {
    auto rhsRestrictor = static_cast<SIInRestrictor const&>(rhs);
    return *_inPredicate == *rhsRestrictor._inPredicate;
}


std::ostream& SIInRestrictor::dbgPrint(std::ostream& os) const {
    os << "SIInRestrictor(" << *_inPredicate << ")";
    return os;
}


std::shared_ptr<query::ColumnRef const> SIInRestrictor::getSecondaryIndexColumnRef() const {
    return _inPredicate->value->getColumnRef();
}


std::string SIInRestrictor::getSILookupQuery(std::string const& secondaryIndexDb,
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
