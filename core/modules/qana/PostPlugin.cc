/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
#include "log/Logger.h"
#include "qana/QueryPlugin.h"
#include "query/OrderByClause.h"
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
    boost::shared_ptr<query::OrderByClause> _orderBy;
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
    if(stmt.hasOrderBy()) {
        _orderBy = stmt.getOrderBy().clone();
    }
}

void
PostPlugin::applyPhysical(QueryPlugin::Plan& p,
                          query::QueryContext& context) {
    // Idea: If a limit is available in the user query, compose a
    // merge statement (if one is not available) and turn on merge
    // fixup.
    if(context.hasChunks()) { // For chunked queries only.
        if((_limit != -1) || _orderBy) { // Aggregating LIMIT or ORDER BY
            // Prepare merge statment.
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
        } // if((_limit != -1) || _orderBy)
        if(_orderBy) {
            // Remove orderby from parallel
            // (no need to sort until we have all the results)
            SelectStmtList::iterator i,e;
            for(i=p.stmtParallel.begin(), e=p.stmtParallel.end(); i != e; ++i) {
                (**i).setOrderBy(boost::shared_ptr<query::OrderByClause>());
            }
            // Make sure the merge has an ORDER BY
            p.stmtMerge.setOrderBy(_orderBy);
        }
    } else { // For non-chunked queries
        LOGGER_INF << "Query is non-chunked\n";
        // Make sure orderby is in the "parallel" section (which is not
        // really parallel). No merge is needed.
        SelectStmtList::iterator i,e;
        for(i=p.stmtParallel.begin(), e=p.stmtParallel.end(); i != e; ++i) {
            (**i).setOrderBy(_orderBy);
        }
    }
}

}}} // namespace lsst::qserv::qana
