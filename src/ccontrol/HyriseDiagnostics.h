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

#ifndef LSST_QSERV_CCONTROL_HYRISEDIAGNOSTICS_H
#define LSST_QSERV_CCONTROL_HYRISEDIAGNOSTICS_H

#include <string>

#include "sql/Expr.h"

namespace lsst::qserv::ccontrol {

/// Diagnostic helpers for translating Hyrise parser enum values into
/// human-readable names, used to produce better error messages.
class HyriseDiagnostics {
public:
    /// Return a human-readable name for a Hyrise expression type.
    static std::string exprTypeName(hsql::ExprType type);

    /// Return a human-readable name for a Hyrise operator type.
    static std::string operatorName(hsql::OperatorType op);
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_HYRISEDIAGNOSTICS_H
