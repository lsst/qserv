// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
#include <string>
#include <vector>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Forward declarations
namespace lsst {
namespace qserv {
namespace qana {
    class QueryMapping;
}
namespace query {
    class SelectStmt;
    class QueryContext;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace qana {

typedef std::vector<boost::shared_ptr<query::SelectStmt> > SelectStmtVector;

/// QueryPlugin is an interface for classes which implement rewrite/optimization
/// rules for incoming SQL queries by operating on query representations.
/// Plugins can act upon the intermediate representation or the concrete plan or both.
/// The QuerySession requests specific QueryPlugins by name and calls them in order.
class QueryPlugin {
public:
    // Types
    class Factory;
    class Plan;
    typedef boost::shared_ptr<QueryPlugin> Ptr;
    typedef boost::shared_ptr<Factory> FactoryPtr;

    virtual ~QueryPlugin() {}

    /// Prepare the plugin for a query
    virtual void prepare() {}

    /// Apply the plugin's actions to the parsed, but not planned query
    virtual void applyLogical(query::SelectStmt& stmt, query::QueryContext&) {}

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(Plan& phy, query::QueryContext& context) {}

    /// Apply the plugins's actions when coverage is known
    virtual void applyFinal(query::QueryContext& context) {}

    /// Lookup a factory for the named type of plugin and construct an instance
    static Ptr newInstance(std::string const& name);

    /// Register a QueryPlugin factory
    static void registerClass(FactoryPtr f);
};

/// Factory is a base class for specific QueryPlugin Factories
class QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<Factory> Ptr;

    virtual ~Factory() {}

    virtual std::string getName() const { return std::string(); }
    virtual QueryPlugin::Ptr newInstance() { return QueryPlugin::Ptr(); }
};

/// A bundle of references to a components that form a "plan"
class QueryPlugin::Plan {
public:
    Plan(query::SelectStmt& stmtOriginal_, SelectStmtVector& stmtParallel_,
         query::SelectStmt& stmtMerge_, bool hasMerge_)
        :  stmtOriginal(stmtOriginal_),
           stmtParallel(stmtParallel_),
          stmtMerge(stmtMerge_),
          hasMerge(hasMerge_) {}

    // Each of these should become a sequence for two-step queries.
    query::SelectStmt& stmtOriginal;
    SelectStmtVector& stmtParallel; //< Group of parallel statements (not a sequence)
    query::SelectStmt& stmtMerge;
    std::string dominantDb;
    boost::shared_ptr<QueryMapping> queryMapping;
    bool const hasMerge;
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_QUERYPLUGIN_H
