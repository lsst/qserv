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
  * @brief Implementation of FuncExpr (a parsed function call
  * expression) and FuncExpr::render
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/FuncExpr.h"

// System headers
#include <iostream>

// Third-party headers
#include "boost/make_shared.hpp"

// Qserv headers
#include "query/ColumnRef.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"

namespace lsst {
namespace qserv {
namespace query {

FuncExpr::Ptr
FuncExpr::newLike(FuncExpr const& src, std::string const& newName) {
    FuncExpr::Ptr e = boost::make_shared<FuncExpr>();
    e->name = newName;
    e->params = src.params; // Shallow list copy.
    return e;
}

FuncExpr::Ptr
FuncExpr::newArg1(std::string const& newName, std::string const& arg1) {
    boost::shared_ptr<ColumnRef> cr = boost::make_shared<ColumnRef>("","",arg1);
    return newArg1(newName,
                   ValueExpr::newSimple(ValueFactor::newColumnRefFactor(cr)));
}

FuncExpr::Ptr
FuncExpr::newArg1(std::string const& newName, ValueExprPtr ve) {
    FuncExpr::Ptr e = boost::make_shared<FuncExpr>();
    e->name = newName;
    e->params.push_back(ve);
    return e;
}

void
FuncExpr::findColumnRefs(ColumnRef::Vector& vector) {
    for(ValueExprPtrVector::iterator i=params.begin();
        i != params.end(); ++i) {
        if(*i) {
            (**i).findColumnRefs(vector);
        }
    }
}

std::ostream&
operator<<(std::ostream& os, FuncExpr const& fe) {
    os << "(" << fe.name << ",";
    output(os, fe.params);
    os << ")";
    return os;
}

std::ostream&
operator<<(std::ostream& os, FuncExpr const* fe) {
    return os << *fe;
}

void
FuncExpr::renderTo(QueryTemplate& qt) const {
    qt.append(name);
    qt.append("(");
    renderList(qt, params);
    qt.append(")");
}

}}} // namespace lsst::qserv::query
