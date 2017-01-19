// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#ifndef LSST_QSERV_QANA_WHEREPLUGIN_H
#define LSST_QSERV_QANA_WHEREPLUGIN_H

// Qserv headers
#include "qana/QueryPlugin.h"


namespace lsst {
namespace qserv {
namespace qana {


/// WherePlugin optimizes out extraneous OR_OP and AND_OP from the
/// WhereClause predicate.
class WherePlugin : public QueryPlugin {
public:
    // Types
    typedef std::shared_ptr<WherePlugin> Ptr;

    WherePlugin() {}
    virtual ~WherePlugin() {}

    void prepare() override {}

    void applyLogical(query::SelectStmt& stmt, query::QueryContext&) override;
    void applyPhysical(QueryPlugin::Plan& p, query::QueryContext&) override {}
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_WHEREPLUGIN_H
