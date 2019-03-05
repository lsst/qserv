// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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
#include "query/CompPredicate.h"

// Qserv headers
#include "ccontrol/UserQueryError.h"
#include "query/ColumnRef.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"


namespace lsst {
namespace qserv {
namespace query {


void CompPredicate::findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {
    if (left) left->findColumnRefs(vector);
    if (right) right->findColumnRefs(vector);
}


std::ostream& CompPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}


void CompPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r.applyToQT(left);
    switch(op) {
        case EQUALS_OP: qt.append("="); break;
        case NULL_SAFE_EQUALS_OP: qt.append("<=>"); break;
        case NOT_EQUALS_OP: qt.append("<>"); break;
        case LESS_THAN_OP: qt.append("<"); break;
        case GREATER_THAN_OP: qt.append(">"); break;
        case LESS_THAN_OR_EQUALS_OP: qt.append("<="); break;
        case GREATER_THAN_OR_EQUALS_OP: qt.append(">="); break;
        case NOT_EQUALS_OP_ALT: qt.append("!="); break;
        default: throw ccontrol::UserQueryBug("Unhandled op:" + std::to_string(op));
    }
    r.applyToQT(right);
}


void CompPredicate::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    vector.push_back(left);
    vector.push_back(right);
}


CompPredicate::OpType CompPredicate::lookupOp(char const* op) {
    switch(op[0]) {
        case '<':
            if (op[1] == '\0') {
                return LESS_THAN_OP;
            } else if (op[1] == '>') {
                return NOT_EQUALS_OP;
            } else if (op[1] == '=') {
                return LESS_THAN_OR_EQUALS_OP;
            }
            throw std::invalid_argument("Invalid op string <?");

        case '>':
            if (op[1] == '\0') {
                return GREATER_THAN_OP;
            } else if (op[1] == '=') {
                return GREATER_THAN_OR_EQUALS_OP;
            }
            throw std::invalid_argument("Invalid op string >?");

        case '=':
            return EQUALS_OP;

        default:
            throw std::invalid_argument("Invalid op string ?");
    }
}


void CompPredicate::dbgPrint(std::ostream& os) const {
    os << "CompPredicate(";
    os << left;
    os << ", " << op;
    os << ", " << right;
    os << ")";
}


bool CompPredicate::operator==(BoolFactorTerm const& rhs) const {
    auto rhsCompPredicate = dynamic_cast<CompPredicate const*>(&rhs);
    if (nullptr == rhsCompPredicate) {
        return false;
    }
    return util::ptrCompare<ValueExpr>(left, rhsCompPredicate->left) &&
           op == rhsCompPredicate->op &&
           util::ptrCompare<ValueExpr>(right, rhsCompPredicate->right);
}


BoolFactorTerm::Ptr CompPredicate::clone() const {
    return std::make_shared<CompPredicate>(left->clone(), op, right->clone());
}


}}} // namespace lsst::qserv::query
