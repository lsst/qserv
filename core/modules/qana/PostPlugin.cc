// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
  * @file
  *
  * @brief PostPlugin does the right thing to handle LIMIT (and
  * perhaps ORDER BY and GROUP BY) clauses.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "qana/PostPlugin.h"

// System headers
#include <cstddef>
#include <stdexcept>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "query/OrderByClause.h"
#include "query/QueryContext.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.PostPlugin");
}

namespace lsst {
namespace qserv {
namespace qana {

////////////////////////////////////////////////////////////////////////
// PostPlugin declaration
////////////////////////////////////////////////////////////////////////
/* &&&
/// PostPlugin is a plugin handling query result post-processing.
class PostPlugin : public QueryPlugin {
public:
    // Types
    typedef std::shared_ptr<PostPlugin> Ptr;

    virtual ~PostPlugin() {}

    /// Prepare the plugin for a query
    virtual void prepare() {}

    /// Apply the plugin's actions to the parsed, but not planned query
    virtual void applyLogical(query::SelectStmt&, query::QueryContext&);

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(QueryPlugin::Plan& plan, query::QueryContext&);

    int _limit;
    std::shared_ptr<query::OrderByClause> _orderBy;
};
*/ // &&&

////////////////////////////////////////////////////////////////////////
// PostPluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class PostPluginFactory : public QueryPlugin::Factory {
public:
    // Types
    typedef std::shared_ptr<PostPluginFactory> Ptr;
    PostPluginFactory() {}
    virtual ~PostPluginFactory() {}

    virtual std::string getName() const { return "Post"; }
    virtual QueryPlugin::Ptr newInstance() {
        QueryPlugin::Ptr p = std::make_shared<PostPlugin>();
        return p;
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
    if (stmt.hasOrderBy()) {
        _orderBy = stmt.getOrderBy().clone();
    }
}

void
PostPlugin::applyPhysical(QueryPlugin::Plan& plan,
                          query::QueryContext& context) {
    // Idea: If a limit is available in the user query, compose a
    // merge statement (if one is not available)
    LOGS(_log, LOG_LVL_DEBUG, "Apply physical");

    if (_limit != NOTSET) {
        // [ORDER BY ...] LIMIT ... is a special case which require sort on worker and sort/aggregation on czar
        if (context.hasChunks()) {
             LOGS(_log, LOG_LVL_DEBUG, "Add merge operation");
             context.needsMerge = true;
         }
    } else if (_orderBy) {
        // If there is no LIMIT clause, remove ORDER BY clause from all Czar queries because it is performed by
        // mysql-proxy (mysql doesn't garantee result order for non ORDER BY queries)
        LOGS(_log, LOG_LVL_TRACE, "Remove ORDER BY from parallel and merge queries: \""
             << *_orderBy << "\"");
        for (auto i = plan.stmtParallel.begin(), e = plan.stmtParallel.end(); i != e; ++i) {
            (**i).setOrderBy(nullptr);
        }
        if (context.needsMerge) {
            plan.stmtMerge.setOrderBy(nullptr);
        }
    }

    if (context.needsMerge) {
        // Prepare merge statement.
        // If empty select in merger, create one with *
        query::SelectList& mList = plan.stmtMerge.getSelectList();
        auto vlist = mList.getValueExprList();
        if (not vlist) {
            throw std::logic_error("Unexpected NULL ValueExpr in SelectList");
        }
        // FIXME: is it really useful to add star if select clause is empty?
        if (vlist->empty()) {
            mList.addStar(std::string());
        }
    }
}

}}} // namespace lsst::qserv::qana
