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
#include "query/CompPredicate.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"
#include "util/IterableFormatter.h"


namespace lsst {
namespace qserv {
namespace query {


std::string SICompRestrictor::_name = "sIndexCompare";


std::ostream& operator<<(std::ostream& os, QsRestrictor const& q) {
    return q.dbgPrint(os);
}


bool QsRestrictor::operator==(const QsRestrictor& rhs) const {
    return typeid(*this) == typeid(rhs) && isEqual(rhs);
}


void QsRestrictorFunction::renderTo(QueryTemplate& qt) const {
    qt.append(_name);
    qt.append("(");
    bool first = true;
    for (auto const& parameter : _params) {
        if (first) {
            first = false;
        } else {
            qt.append(",");
        }
        qt.append(parameter);
    }
    qt.append(")");
}


bool QsRestrictorFunction::isEqual(const QsRestrictor& rhs) const {
    auto rhsRestrictorFunc = static_cast<QsRestrictorFunction const&>(rhs);
    if (_name != rhsRestrictorFunc._name) return false;
    return _params == rhsRestrictorFunc._params;
}


std::ostream& QsRestrictorFunction::dbgPrint(std::ostream& os) const {
    // todo this needs to change to QsRestrictorFunction
    os << "QsRestrictor(" << "\"" <<  _name << "\"";
    os << ", " << util::printable(_params, "", "");
    os << ")";
    return os;
}


void SICompRestrictor::renderTo(QueryTemplate& qt) const {
    _compPredicate->renderTo(qt);
}


bool SICompRestrictor::isEqual(const QsRestrictor& rhs) const {
    auto rhsRestrictorFunc = static_cast<SICompRestrictor const&>(rhs);
    if (_name != rhsRestrictorFunc._name) return false;
    return _compPredicate == rhsRestrictorFunc._compPredicate;
}


std::ostream& SICompRestrictor::dbgPrint(std::ostream& os) const {
    os << "SICompRestrictor(" << "\"" <<  _name << "\"";
    os << ", " << *_compPredicate;
    os << ")";
    return os;
}


std::shared_ptr<query::ColumnRef const> SICompRestrictor::getSecondaryIndexColumn() const {
    return _useLeft ? _compPredicate->left->getColumnRef() : _compPredicate->right->getColumnRef();
}


void SICompRestrictor::setSecondaryIndexTableRef(std::string db, std::string table) {
    auto tableRef = std::make_shared<query::TableRef>(db, table, std::string());
    if (_useLeft) {
        _compPredicate->left->getColumnRef()->setTable(tableRef);
    } else {
        _compPredicate->right->getColumnRef()->setTable(tableRef);
    }
}


}}} // namespace lsst::qserv::query
