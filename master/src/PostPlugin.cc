/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
// PostPlugin does the right thing to handle LIMIT (and perhaps ORDER
// BY and GROUP BY) clauses. 
#include "lsst/qserv/master/PostPlugin.h"
#include <string>

#include "lsst/qserv/master/QueryContext.h"
#include "lsst/qserv/master/QueryPlugin.h"
#include "lsst/qserv/master/SelectList.h"
#include "lsst/qserv/master/SelectStmt.h"
#if 0
#include "lsst/qserv/master/FromList.h"
#include "lsst/qserv/master/WhereClause.h"
#include "lsst/qserv/master/FuncExpr.h"
#include "lsst/qserv/master/SphericalBoxStrategy.h"
#include "lsst/qserv/master/TableRefChecker.h"
#include "lsst/qserv/master/TableNamer.h"
#include "lsst/qserv/master/QueryMapping.h"
#endif
namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
} // anonymous

namespace lsst { namespace qserv { namespace master {


////////////////////////////////////////////////////////////////////////
// PostPlugin declaration
////////////////////////////////////////////////////////////////////////
class PostPlugin : public lsst::qserv::master::QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<PostPlugin> Ptr;
    
    virtual ~PostPlugin() {}

    /// Prepare the plugin for a query
    virtual void prepare() {}

    /// Apply the plugin's actions to the parsed, but not planned query
    virtual void applyLogical(SelectStmt& stmt, QueryContext&);

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(QueryPlugin::Plan& p, QueryContext& context);
    
    int _limit;
};

////////////////////////////////////////////////////////////////////////
// PostPluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class PostPluginFactory : public lsst::qserv::master::QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<PostPluginFactory> Ptr;
    PostPluginFactory() {}
    virtual ~PostPluginFactory() {}

    virtual std::string getName() const { return "Post"; }
    virtual lsst::qserv::master::QueryPlugin::Ptr newInstance() {
        return lsst::qserv::master::QueryPlugin::Ptr(new PostPlugin());
    }
};

////////////////////////////////////////////////////////////////////////
// registarPostPlugin implementation
////////////////////////////////////////////////////////////////////////
// factory registration
void 
registerPostPlugin() {
    PostPluginFactory::Ptr f(new PostPluginFactory());
    QueryPlugin::registerClass(f);
}

////////////////////////////////////////////////////////////////////////
// PostPlugin implementation
////////////////////////////////////////////////////////////////////////
void 
PostPlugin::applyLogical(SelectStmt& stmt, QueryContext& context) {
    _limit = stmt.getLimit();
}

void
PostPlugin::applyPhysical(QueryPlugin::Plan& p, QueryContext& context) {
    // Idea: If a limit is available in the user query, compose a
    // merge statement (if one is not available) and turn on merge
    // fixup.
    if(_limit != -1) { // Make sure merge statement is setup for LIMIT
        // If empty select in merger, create one with *
        SelectList& mList = p.stmtMerge.getSelectList();
        boost::shared_ptr<ValueExprList> vlist;
        vlist = mList.getValueExprList();
        assert(vlist.get());
        if(vlist->size() == 0) {
            mList.addStar(antlr::RefAST()); 
        }
    // Patch MergeFixup.
    context.needsMerge = true;
    } // if limit != -1
}

}}} // namespace lsst::qserv::master
