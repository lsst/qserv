// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
 * @ingroup WRITE MODULE HERE
 *
 * @brief WRITE DESCRIPTION HERE
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

// Class header
#include "qana/DuplicateSelectExprPlugin.h"

// System headers

// Third-party headers

// Qserv headers
#include "qana/AnalysisError.h"
#include "query/SelectList.h"

namespace lsst {
namespace qserv {
namespace qana {

void DuplicateSelectExprPlugin::applyLogical(query::SelectStmt& stmt, query::QueryContext&) {
    LOGF_INFO("DuplicateSelectExprPlugin::applyLogical");
    std::ostringstream stream;
    query::SelectList const& selectList = stmt.getSelectList();
    selectList.dbgPrint(stream);
    LOGF_INFO("%s" % stream.str());
    if (false) {
        throw qana::AnalysisBug("Duplicate Select Expr, rewrite SQL query using as");
    }
}

////////////////////////////////////////////////////////////////////////
// DuplicateSelectExprPluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class DuplicateSelectExprPluginFactory : public QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<DuplicateSelectExprPluginFactory> Ptr;
    DuplicateSelectExprPluginFactory() {}
    virtual ~DuplicateSelectExprPluginFactory() {}

    virtual std::string getName() const { return "DuplicateSelectExpr"; }
    virtual QueryPlugin::Ptr newInstance() {
        return boost::make_shared<DuplicateSelectExprPlugin>();
    }
};

////////////////////////////////////////////////////////////////////////
// registerTablePlugin implementation
////////////////////////////////////////////////////////////////////////
namespace {
struct registerPlugin {
    registerPlugin() {
        DuplicateSelectExprPluginFactory::Ptr f = boost::make_shared<DuplicateSelectExprPluginFactory>();
        QueryPlugin::registerClass(f);
    }
};
// Static registration
registerPlugin registerDuplicateSelectExprPlugin;
} // annonymous namespace

}}} // namespace lsst::qserv::qana
