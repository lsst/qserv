// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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

#ifndef LSST_QSERV_CCONTROL_USERQUERYRESOURCES_H
#define LSST_QSERV_CCONTROL_USERQUERYRESOURCES_H

#include <memory>
#include <string>

#include "qmeta/types.h"
#include "mysql/MySqlConfig.h"

namespace lsst::qserv::ccontrol {
class UserQueryResources;
}

namespace lsst::qserv::css {
class CssAccess;
}

namespace lsst::qserv::mysql {
class MySqlConfig;
}

namespace lsst::qserv::qmeta {
class QMeta;
class QStatus;
class QMetaSelect;
}  // namespace lsst::qserv::qmeta

namespace lsst::qserv::qproc {
class DatabaseModels;
class SecondaryIndex;
}  // namespace lsst::qserv::qproc

namespace lsst::qserv::ccontrol {

/**
 * UserQueryResources owns the Czar resources that are useful to & shared among UserQuery instances.
 */
class UserQuerySharedResources {
public:
    UserQuerySharedResources(std::shared_ptr<css::CssAccess> const& css_,
                             mysql::MySqlConfig const& mysqlResultConfig_,
                             std::shared_ptr<qproc::SecondaryIndex> const& secondaryIndex_,
                             std::shared_ptr<qmeta::QMeta> const& queryMetadata_,
                             std::shared_ptr<qmeta::QStatus> const& queryStatsData_,
                             std::shared_ptr<qmeta::QMetaSelect> const& qMetaSelect_,
                             std::shared_ptr<qproc::DatabaseModels> const& databaseModels_,
                             std::string const& czarName, int interactiveChunkLimit_);

    UserQuerySharedResources(UserQuerySharedResources const& rhs) = default;
    UserQuerySharedResources& operator=(UserQuerySharedResources const& rhs) = delete;
    std::shared_ptr<css::CssAccess> css;
    mysql::MySqlConfig const mysqlResultConfig;
    std::shared_ptr<qproc::SecondaryIndex> secondaryIndex;
    std::shared_ptr<qmeta::QMeta> queryMetadata;
    std::shared_ptr<qmeta::QStatus> queryStatsData;
    std::shared_ptr<qmeta::QMetaSelect> qMetaSelect;
    std::shared_ptr<qproc::DatabaseModels> databaseModels;
    qmeta::CzarId qMetaCzarId;  ///< Czar ID in QMeta database
    int const interactiveChunkLimit;

    /**
     * @brief Make a query resources with parameters that are specific to the UserQuery (the id and the
     *        result database), that also has access to the shared parameters in the
     *        UserQuerySharedResources.
     *
     * @param userQueryId the query id specific to the UserQuery.
     * @param resultDb the result db specifically for the UserQuery
     * @return std::shared_ptr<UserQueryResources> The pointer to the new object.
     */
    std::shared_ptr<UserQueryResources> makeUserQueryResources(std::string const& userQueryId,
                                                               std::string const& resultDb);
};

/**
 * UserQueryResources owns the parameters specific to a single UserQuery and contains the shared resources
 * that are useful to & shared among UserQuery instances.
 */
class UserQueryResources : public UserQuerySharedResources {
public:
    UserQueryResources(UserQuerySharedResources const& userQuerySharedResources,
                       std::string const& userQueryId_, std::string const& resultDb_)
            : UserQuerySharedResources(userQuerySharedResources),
              userQueryId(userQueryId_),
              resultDb(resultDb_) {}

    std::string userQueryId;
    std::string resultDb;
};

}  // namespace lsst::qserv::ccontrol

#endif  // LSST_QSERV_CCONTROL_USERQUERYRESOURCES_H
