// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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

// Class header
#include "ccontrol/UserQueryResources.h"

#include "qmeta/QMeta.h"

namespace lsst {
namespace qserv {
namespace ccontrol {


UserQuerySharedResources::UserQuerySharedResources(std::shared_ptr<css::CssAccess> css_,
                                                   mysql::MySqlConfig const& mysqlResultConfig_,
                                                   std::shared_ptr<qproc::SecondaryIndex> secondaryIndex_,
                                                   std::shared_ptr<qmeta::QMeta> queryMetadata_,
                                                   std::shared_ptr<qmeta::QStatus> queryStatsData_,
                                                   std::shared_ptr<qmeta::QMetaSelect> qMetaSelect_,
                                                   std::shared_ptr<sql::SqlConnection> resultDbConn_,
                                                   std::string const& czarName)
        : css(css_),
        mysqlResultConfig(mysqlResultConfig_),
        secondaryIndex(secondaryIndex_),
        queryMetadata(queryMetadata_),
        queryStatsData(queryStatsData_),
        qMetaSelect(qMetaSelect_),
        resultDbConn(resultDbConn_)
{
    // register czar in QMeta
    // TODO: check that czar with the same name is not active already?
    qMetaCzarId = queryMetadata->registerCzar(czarName);
}


std::shared_ptr<UserQueryResources> UserQuerySharedResources::makeUserQueryResources(
        std::string const& userQueryId, std::string const& resultDb) {
    return std::make_shared<UserQueryResources>(*this, userQueryId, resultDb);
}


}}}
