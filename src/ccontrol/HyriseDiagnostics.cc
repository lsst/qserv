// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2026 LSST.
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

#include "ccontrol/HyriseDiagnostics.h"

namespace lsst::qserv::ccontrol {

std::string HyriseDiagnostics::exprTypeName(hsql::ExprType type) {
    switch (type) {
        case hsql::kExprCast:
            return "CAST expression";
        case hsql::kExprExtract:
            return "EXTRACT expression";
        case hsql::kExprSelect:
            return "scalar subquery";
        case hsql::kExprParameter:
            return "parameter placeholder";
        case hsql::kExprArray:
            return "array literal";
        case hsql::kExprArrayIndex:
            return "array subscript";
        case hsql::kExprLiteralDate:
            return "date literal";
        case hsql::kExprLiteralInterval:
            return "interval literal";
        case hsql::kExprHint:
            return "optimizer hint";
        default:
            return "expression type " + std::to_string(type);
    }
}

std::string HyriseDiagnostics::operatorName(hsql::OperatorType op) {
    switch (op) {
        case hsql::kOpILike:
            return "ILIKE";
        case hsql::kOpExists:
            return "EXISTS";
        case hsql::kOpCase:
            return "CASE";
        case hsql::kOpNot:
            return "NOT";
        case hsql::kOpLike:
            return "LIKE";
        case hsql::kOpAnd:
            return "AND";
        case hsql::kOpOr:
            return "OR";
        case hsql::kOpIn:
            return "IN";
        case hsql::kOpBetween:
            return "BETWEEN";
        case hsql::kOpIsNull:
            return "IS NULL";
        default:
            return "operator type " + std::to_string(op);
    }
}

}  // namespace lsst::qserv::ccontrol
