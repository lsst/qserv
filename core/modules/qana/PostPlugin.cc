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
/**
  * @file PostPlugin.cc
  *
  * @brief PostPlugin does the right thing to handle LIMIT (and
  * perhaps ORDER BY and GROUP BY) clauses.
  *
  * @author Daniel L. Wang, SLAC
  */
// No public interface (no PostPlugin.h)

// System headers
#include <stdexcept>
#include <string>

// Local headers
#include "qana/QueryPlugin.h"
#include "query/QueryContext.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"

namespace lsst {
namespace qserv {
namespace qana {

////////////////////////////////////////////////////////////////////////
// PostPlugin declaration
////////////////////////////////////////////////////////////////////////
/// PostPlugin is a plugin handling query result post-processing.
class PostPlugin : public QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<PostPlugin> Ptr;

    virtual ~PostPlugin() {}

    /// Prepare the plugin for a query
    virtual void prepare() {}

    /// Apply the plugin's actions to the parsed, but not planned query
    virtual void applyLogical(query::SelectStmt&, query::QueryContext&);

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(QueryPlugin::Plan& p, query::QueryContext&);

    int _limit;
};

////////////////////////////////////////////////////////////////////////
// PostPluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class PostPluginFactory : public QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<PostPluginFactory> Ptr;
    PostPluginFactory() {}
    virtual ~PostPluginFactory() {}

    virtual std::string getName() const { return "Post"; }
    virtual QueryPlugin::Ptr newInstance() {
        return QueryPlugin::Ptr(new PostPlugin());
    }
};

////////////////////////////////////////////////////////////////////////
// registerPostPlugin implementation
////////////////////////////////////////////////////////////////////////
namespace {
struct registerPlugin {
    registerPlugin() {
    PostPluginFactory::Ptr f(new PostPluginFactory());
    QueryPlugin::registerClass(f);
    }
};
// Static registration
registerPlugin registerPostPlugin;

} // annonymous namespace

////////////////////////////////////////////////////////////////////////
// PostPlugin implementation
////////////////////////////////////////////////////////////////////////
void
PostPlugin::applyLogical(query::SelectStmt& stmt, 
                         query::QueryContext& context) {
    _limit = stmt.getLimit();
}

void
PostPlugin::applyPhysical(QueryPlugin::Plan& p, 
                          query::QueryContext& context) {
    // Idea: If a limit is available in the user query, compose a
    // merge statement (if one is not available) and turn on merge
    // fixup.
    if(_limit != -1) { // Make sure merge statement is setup for LIMIT
        // If empty select in merger, create one with *
        query::SelectList& mList = p.stmtMerge.getSelectList();
        boost::shared_ptr<query::ValueExprList> vlist;
        vlist = mList.getValueExprList();
        if(!vlist) {
            throw std::logic_error("Unexpected NULL ValueExpr in SelectList");
        }
        if(vlist->size() == 0) {
            mList.addStar(std::string());
        }
        // Patch MergeFixup.
        context.needsMerge = true;
    } // if limit != -1
}

}}} // namespace lsst::qserv::qana
