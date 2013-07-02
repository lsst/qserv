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
#ifndef LSST_QSERV_MASTER_QUERYPLUGIN_H
#define LSST_QSERV_MASTER_QUERYPLUGIN_H
/**
  * @file 
  *
  * @author Daniel L. Wang, SLAC
  */
#include <list>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
// Forward
class QueryContext;
class QueryMapping;
class SelectStmt; 

typedef std::list<boost::shared_ptr<SelectStmt> > SelectStmtList;
 
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
    virtual void applyLogical(SelectStmt& stmt, QueryContext&) {}

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(Plan& phy, QueryContext& context) {} 

    /// Lookup a factory for the named type of plugin and construct an instance
    static Ptr newInstance(std::string const& name);

    /// Register a QueryPlugin factory
    static void registerClass(FactoryPtr f);
};

/// Factory is an abstract class for specific QueryPlugin Factories
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
    Plan(SelectStmt& stmtOriginal_, SelectStmt& stmtParallel_, 
         SelectStmt& stmtMerge_, bool& hasMerge_) 
        :  stmtOriginal(stmtOriginal_),
           stmtParallel(stmtParallel_), 
          stmtMerge(stmtMerge_), 
          hasMerge(hasMerge_) {}

    // Each of these should become a sequence for two-step queries.
    SelectStmt& stmtOriginal; 
    SelectStmt& stmtParallel; 
    SelectStmt& stmtMerge; 
    std::string dominantDb;
    boost::shared_ptr<QueryMapping> queryMapping;
    bool hasMerge;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_QUERYPLUGIN_H

