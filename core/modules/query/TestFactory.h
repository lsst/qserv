// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_QUERY_TESTFACTORY_H
#define LSST_QSERV_QUERY_TESTFACTORY_H

// System headers
#include <memory>

// Qserv headers
#include "global/stringTypes.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace css {
    class CssAccess;
}
namespace query {
    class QueryContext;
    class SelectStmt;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace query {

/// TestFactory is a factory for non-parsed query representation objects
class TestFactory {
public:
    TestFactory() {}
    std::shared_ptr<QueryContext> newContext();
    std::shared_ptr<QueryContext> newContext(
                         std::shared_ptr<css::CssAccess> css);
    std::shared_ptr<SelectStmt> newSimpleStmt();
    std::shared_ptr<SelectStmt> newDuplSelectExprStmt();
private:
    static void addSelectField(std::shared_ptr<SelectStmt> const& stmt, StringVector const& fields);
    static void addFrom(std::shared_ptr<SelectStmt> const& stmt);
    static void addWhere(std::shared_ptr<SelectStmt> const& stmt);
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_TESTFACTORY_H
