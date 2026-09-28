// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_QANA_MATCHTABLEPLUGIN_H
#define LSST_QSERV_QANA_MATCHTABLEPLUGIN_H

// Qserv headers
#include "qana/QueryPlugin.h"

namespace lsst::qserv::qana {

/// Rewrites queries on match tables so that they do not return duplicate rows potentially introduced by the
/// partitioning process. This applies to match tables alone or those that have been joined to replicated
/// tables. Joins with other partitioned tables are handled by TablePlugin. This plugin assumes that
/// TablePlugin has already run beforehand.
class MatchTablePlugin : public QueryPlugin {
public:
    typedef std::shared_ptr<MatchTablePlugin> Ptr;

    MatchTablePlugin() {}
    virtual ~MatchTablePlugin() {}

    void prepare() override {}
    void applyLogical(query::SelectStmt& stmt, query::QueryContext& ctx) override;
    void applyPhysical(QueryPlugin::Plan& p, query::QueryContext& ctx) override {}

    /// Return the name of the plugin class for logging.
    std::string name() const override { return "MatchTablePlugin"; }
};

}  // namespace lsst::qserv::qana

#endif /* LSST_QSERV_QANA_MATCHTABLEPLUGIN_H */
