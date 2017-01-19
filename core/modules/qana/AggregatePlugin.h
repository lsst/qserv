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
#ifndef LSST_QSERV_QANA_AGGREGATEPLUGIN_H
#define LSST_QSERV_QANA_AGGREGATEPLUGIN_H

// Qserv headers
#include "qana/QueryPlugin.h"
#include "query/AggOp.h"

namespace lsst {
namespace qserv {
namespace qana {

/// AggregatePlugin primarily operates in
/// the second phase of query manipulation. It rewrites the
/// select-list of a query in their parallel and merging instances so
/// that a SUM() becomes a SUM() followed by another SUM(), AVG()
/// becomes SUM() and COUNT() followed by SUM()/SUM(), etc.
class AggregatePlugin : public QueryPlugin {
public:
    // Types
    typedef std::shared_ptr<AggregatePlugin> Ptr;

    virtual ~AggregatePlugin() {}

    virtual void prepare() {}

    virtual void applyPhysical(QueryPlugin::Plan& plan,
                               query::QueryContext&);
private:
    query::AggOp::Mgr _aMgr;
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_AGGREGATEPLUGIN_H
