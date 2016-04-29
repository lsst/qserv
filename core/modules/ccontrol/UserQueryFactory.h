// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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

#ifndef LSST_QSERV_CCONTROL_USERQUERYFACTORY_H
#define LSST_QSERV_CCONTROL_USERQUERYFACTORY_H
/**
  * @file
  *
  * @brief Factory for UserQuery.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <cstdint>
#include <memory>

// Third-party headers
#include "boost/utility.hpp"

// Local headers
#include "ccontrol/UserQuery.h"
#include "global/stringTypes.h"
#include "util/ConfigStore.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

///  UserQueryFactory breaks construction of user queries into two phases:
///  creation/configuration of the factory and construction of the
///  UserQuery. This facilitates re-use of initialized state that is usually
///  constant between successive user queries.
class UserQueryFactory : private boost::noncopyable {
public:

    UserQueryFactory(util::ConfigStore const& config,
                     std::string const& czarName);

    /// @param query:       Query text
    /// @param defaultDb:   Default database name, may be empty
    /// @return new UserQuery object
    UserQuery::Ptr newUserQuery(std::string const& query,
                                std::string const& defaultDb);

private:
    class Impl;
    std::shared_ptr<Impl> _impl;
};

}}} // namespace lsst::qserv:control

#endif // LSST_QSERV_CCONTROL_USERQUERYFACTORY_H
