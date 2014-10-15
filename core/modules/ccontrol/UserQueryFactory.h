// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

// Third-party headers
#include "boost/shared_ptr.hpp"
#include "boost/utility.hpp"

// Local headers
#include "global/stringTypes.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace css {
  class KvInterface;
}}}

namespace lsst {
namespace qserv {
namespace ccontrol {

///  UserQueryFactory breaks construction of user queries into two phases:
///  creation/configuration of the factory and construction of the
///  UserQuery. This facilitates re-use of initialized state that is usually
///  constant between successive user queries.
class UserQueryFactory : private boost::noncopyable {
public:
    typedef boost::shared_ptr<lsst::qserv::css::KvInterface> KviPtr;
    UserQueryFactory(std::map<std::string,std::string> const& m,
                     KviPtr kvi=KviPtr());

    /// @return a handle to the new UserQuery object to be used with the
    /// userQueryProxy interface.
    int newUserQuery(std::string const& query,
                     std::string const& defaultDb,
                     std::string const& resultTable);
private:
    class Impl;
    boost::shared_ptr<Impl> _impl;
};

}}} // namespace lsst::qserv:control

#endif // LSST_QSERV_CCONTROL_USERQUERYFACTORY_H
