// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
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
#ifndef LSST_QSERV_QANA_QUERYPLUGIN_H
#define LSST_QSERV_QANA_QUERYPLUGIN_H
/**
 * @file
 *
 * @author Daniel L. Wang, SLAC
 */

// System headers
#include <memory>
#include <string>
#include <vector>

// Local headers
#include "query/typedefs.h"

// Forward declarations
namespace lsst::qserv::query {
class SelectStmt;
class QueryContext;
}  // namespace lsst::qserv::query

namespace lsst::qserv::qana {

using query::SelectStmtPtrVector;

/// QueryPlugin is an interface for classes which implement rewrite/optimization
/// rules for incoming SQL queries by operating on query representations.
/// Plugins can act upon the intermediate representation or the concrete plan or both.
/// The QuerySession requests specific QueryPlugins by name and calls them in order.
class QueryPlugin {
public:
    // Types
    class Factory;
    class Plan;
    typedef std::shared_ptr<QueryPlugin> Ptr;
    typedef std::shared_ptr<Factory> FactoryPtr;

    virtual ~QueryPlugin() {}

    /// Prepare the plugin for a query
    virtual void prepare() {}

    /// Apply the plugin's actions to the parsed, but not planned query
    virtual void applyLogical(query::SelectStmt& stmt, query::QueryContext&) {}

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(Plan& phy, query::QueryContext& context) {}

    /// Apply the plugins's actions when coverage is known
    virtual void applyFinal(query::QueryContext& context) {}

    /// Return the name of the plugin class for logging.
    virtual std::string name() const = 0;
};

/// A bundle of references to a components that form a "plan"
class QueryPlugin::Plan {
public:
    /**
     * @brief Construct a new Plan object
     *
     * Objects that are stored by reference are owned by the caller and are changed by the plugins.
     *
     * @param stmtOriginal_ the original SelectStmt
     * @param stmtParallel_ the SelectStmt that gets sent to workers.
     * @param stmtPreFlight_ the SelectStmt that is used locally to get schema to create the result table.
     * @param stmtMerge_ the aggregation SelectStmt run on the merge table after results are delivered.
     * @param hasMerge_ if there is an aggregation step to perform.
     */
    Plan(query::SelectStmt& stmtOriginal_, SelectStmtPtrVector& stmtParallel_,
         std::shared_ptr<query::SelectStmt>& stmtPreFlight_, query::SelectStmt& stmtMerge_, bool hasMerge_)
            : stmtOriginal(stmtOriginal_),
              stmtParallel(stmtParallel_),
              stmtPreFlight(stmtPreFlight_),
              stmtMerge(stmtMerge_),
              hasMerge(hasMerge_) {}

    // Each of these should become a sequence for two-step queries.
    query::SelectStmt& stmtOriginal;

    // Group of parallel statements (not a sequence)
    SelectStmtPtrVector& stmtParallel;

    // The statement used to 'preflight' the worker queries, to get the schema to generate the result table.
    std::shared_ptr<query::SelectStmt>& stmtPreFlight;

    // The statement used to run the aggregation step on the merge table.
    query::SelectStmt& stmtMerge;

    std::string dominantDb;

    // True if an aggregation step must be performed on the merge table after worker queries
    // complete.
    bool const hasMerge;
};

}  // namespace lsst::qserv::qana

#endif  // LSST_QSERV_QANA_QUERYPLUGIN_H
