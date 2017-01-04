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
#ifndef LSST_QSERV_QANA_POSTPLUGIN_H
#define LSST_QSERV_QANA_POSTPLUGIN_H

// Qserv headers
#include "qana/QueryPlugin.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {

class OrderByClause;

}}} // namespace lsst::qserv::query


namespace lsst {
namespace qserv {
namespace qana {

/// PostPlugin is a plugin handling query result post-processing.
class PostPlugin : public QueryPlugin {
public:
    // Types
    typedef std::shared_ptr<PostPlugin> Ptr;

    virtual ~PostPlugin() {}

    /// Prepare the plugin for a query
    void prepare() override {}

    /// Apply the plugin's actions to the parsed, but not planned query
    void applyLogical(query::SelectStmt&, query::QueryContext&) override;

    /// Apply the plugins's actions to the concrete query plan.
    void applyPhysical(QueryPlugin::Plan& plan, query::QueryContext&) override;

    int _limit;
    std::shared_ptr<query::OrderByClause> _orderBy;
};

}}} // namespace lsst::qserv::qana


#endif // LSST_QSERV_QANA_POSTPLUGIN_H
