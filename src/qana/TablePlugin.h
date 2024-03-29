// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#ifndef LSST_QSERV_QANA_TABLEPLUGIN_H
#define LSST_QSERV_QANA_TABLEPLUGIN_H

// Qserv headers
#include "qana/QueryPlugin.h"

// Forward declarations
namespace lsst::qserv::query {
class TableRef;
}  // namespace lsst::qserv::query

namespace lsst::qserv::qana {

class QueryMapping;

/// TablePlugin is a query plugin that inserts place holders for table
/// name substitution.
class TablePlugin : public QueryPlugin {
public:
    // Types
    typedef std::shared_ptr<TablePlugin> Ptr;

    TablePlugin() : _nextValueExprAlias(0), _nextTableRefAlias(0) {}
    virtual ~TablePlugin() {}

    void prepare() override {}

    void applyLogical(query::SelectStmt& stmt, query::QueryContext& context) override;
    void applyPhysical(QueryPlugin::Plan& p, query::QueryContext& context) override;

    /// Return the name of the plugin class for logging.
    std::string name() const override { return "TablePlugin"; }

private:
    std::string _getNextValueExprAlias();
    std::string _getNextTableRefAlias();

    void _setAlias(std::shared_ptr<query::TableRef> const& tableRef, query::QueryContext& context);

    int _rewriteTables(SelectStmtPtrVector& outList, query::SelectStmt& in, query::QueryContext& context,
                       std::shared_ptr<qana::QueryMapping>& mapping);

    unsigned int _nextValueExprAlias;
    unsigned int _nextTableRefAlias;
    std::string _dominantDb;
};

}  // namespace lsst::qserv::qana

#endif  // LSST_QSERV_QANA_TABLEPLUGIN_H
