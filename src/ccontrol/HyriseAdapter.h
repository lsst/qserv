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

#ifndef LSST_QSERV_CCONTROL_HYRISEADAPTER_H
#define LSST_QSERV_CCONTROL_HYRISEADAPTER_H

#include <memory>
#include <string>

namespace lsst::qserv::query {
class SelectStmt;
}  // namespace lsst::qserv::query

namespace lsst::qserv::ccontrol {

/// Build Qserv query IR from Hyrise parser output.
class HyriseAdapter {
public:
    /// Parse @p sql and return the Qserv query IR for the single SELECT statement it contains.
    /// @throws parser::ParseException on parse errors or unsupported constructs.
    static std::shared_ptr<query::SelectStmt> makeSelectStmt(std::string const& sql);
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_HYRISEADAPTER_H
