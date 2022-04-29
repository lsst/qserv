// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST.
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

#ifndef LSST_QSERV_CCONTROL_PARSEHELPERS_H
#define LSST_QSERV_CCONTROL_PARSEHELPERS_H

#include <cxxabi.h>
#include <string>

#include "antlr4-runtime.h"

namespace lsst { namespace qserv { namespace ccontrol {

// get the query string for the portion of the query represented in the given context
static std::string getQueryString(antlr4::ParserRuleContext* ctx) {
    return ctx->getStart()->getInputStream()->getText(
            antlr4::misc::Interval(ctx->getStart()->getStartIndex(), ctx->getStop()->getStopIndex()));
}

template <typename T>
std::string getTypeName() {
    int status;
    return abi::__cxa_demangle(typeid(T).name(), 0, 0, &status);
}

template <typename T>
std::string getTypeName(T obj) {
    int status;
    return abi::__cxa_demangle(typeid(obj).name(), 0, 0, &status);
}

}}}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_PARSEHELPERS_H
