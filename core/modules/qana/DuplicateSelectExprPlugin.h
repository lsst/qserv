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

// Parent class
#include "qana/QueryPlugin.h"

// System headers

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/QueryContext.h"
#include "query/SelectStmt.h"

#ifndef LSST_QSERV_QANA_DUPLICATESELECTEXPRPLUGIN_H
#define LSST_QSERV_QANA_DUPLICATESELECTEXPRPLUGIN_H

namespace lsst {
namespace qserv {
namespace qana {

class DuplicateSelectExprPlugin : public QueryPlugin {
public:
    virtual ~DuplicateSelectExprPlugin() {}

    virtual void applyLogical(query::SelectStmt& stmt, query::QueryContext&);

};

}}} // namespace lsst::qserv::qana


#endif
