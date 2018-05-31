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
namespace lsst {
namespace qserv {
namespace query {
    class SelectStmt;
    class QueryContext;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace qana {

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
};


/// A bundle of references to a components that form a "plan"
class QueryPlugin::Plan {
public:
    Plan(query::SelectStmt& stmtOriginal_, SelectStmtPtrVector& stmtParallel_,
         query::SelectStmt& stmtMerge_, bool hasMerge_)
        :  stmtOriginal(stmtOriginal_),
           stmtParallel(stmtParallel_),
          stmtMerge(stmtMerge_),
          hasMerge(hasMerge_) {}

    // Each of these should become a sequence for two-step queries.
    query::SelectStmt& stmtOriginal;
    SelectStmtPtrVector& stmtParallel; //< Group of parallel statements (not a sequence)
    query::SelectStmt& stmtMerge;
    std::string dominantDb;
    bool const hasMerge;
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_QUERYPLUGIN_H
