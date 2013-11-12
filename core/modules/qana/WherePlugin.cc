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
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */
// No public interface (no WherePlugin.h)
#include <string>
#include <stdexcept>
#include "util/common.h"

#include "query/BoolTerm.h"
#include "qana/QueryPlugin.h"
#include "query/SelectStmt.h"
#include "query/WhereClause.h"

namespace lsst {
namespace qserv {
namespace master {

////////////////////////////////////////////////////////////////////////
// WherePlugin declaration
////////////////////////////////////////////////////////////////////////
/// WherePlugin optimizes out extraneous OR_OP and AND_OP from the
// WhereClause predicate.
class WherePlugin : public QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<WherePlugin> Ptr;

    virtual ~WherePlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(SelectStmt& stmt, QueryContext&);
    virtual void applyPhysical(QueryPlugin::Plan& p, QueryContext&) {}
};

////////////////////////////////////////////////////////////////////////
// WherePluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class WherePluginFactory : public QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<WherePluginFactory> Ptr;
    WherePluginFactory() {}
    virtual ~WherePluginFactory() {}

    virtual std::string getName() const { return "Where"; }
    virtual lsst::qserv::master::QueryPlugin::Ptr newInstance() {
        return lsst::qserv::master::QueryPlugin::Ptr(new WherePlugin());
    }
};

////////////////////////////////////////////////////////////////////////
// registerWherePlugin implementation
////////////////////////////////////////////////////////////////////////
namespace {
struct registerPlugin {
    registerPlugin() {
        WherePluginFactory::Ptr f(new WherePluginFactory());
        lsst::qserv::master::QueryPlugin::registerClass(f);
    }
};
// Static registration
registerPlugin registerWherePlugin;
}

void WherePlugin::applyLogical(SelectStmt& stmt, QueryContext&) {
    // Go to the WhereClause and remove extraneous OR_OP and AND_OP,
    // except for the root AND.
    if(!stmt.hasWhereClause()) { return; }

    WhereClause& wc = stmt.getWhereClause();
    boost::shared_ptr<AndTerm> at = wc.getRootAndTerm();
    if(!at) { return; }
    typedef BoolTerm::PtrList::iterator Iter;
    for(Iter i=at->iterBegin(), e=at->iterEnd(); i != e; ++i) {
        boost::shared_ptr<BoolTerm> reduced = (**i).getReduced();
        if(reduced) {
            *i = reduced;
        }
    }

}

}}} // lsst::qserv::master
