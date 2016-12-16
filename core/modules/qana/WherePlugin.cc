// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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

// Parent class
#include "qana/WherePlugin.h"

// System headers
#include <stdexcept>
#include <string>
#include "util/common.h"

// Third-party headers

// Qserv headers
#include "query/BoolTerm.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/WhereClause.h"


namespace lsst {
namespace qserv {
namespace qana {

////////////////////////////////////////////////////////////////////////
// WherePlugin declaration
////////////////////////////////////////////////////////////////////////

void
WherePlugin::applyLogical(query::SelectStmt& stmt, query::QueryContext&) {
    // Go to the WhereClause and remove extraneous OR_OP and AND_OP,
    // except for the root AND.
    if (!stmt.hasWhereClause()) { return; }

    query::WhereClause& wc = stmt.getWhereClause();
    std::shared_ptr<query::AndTerm> at = wc.getRootAndTerm();
    if (!at) { return; }
    typedef query::BoolTerm::PtrVector::iterator Iter;
    for(Iter i=at->iterBegin(), e=at->iterEnd(); i != e; ++i) {
        std::shared_ptr<query::BoolTerm> reduced = (**i).getReduced();
        if (reduced) {
            *i = reduced;
        }
    }
}

}}} // namespace lsst::qserv::qana
