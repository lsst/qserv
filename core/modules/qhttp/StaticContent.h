/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

#ifndef LSST_QSERV_QHTTP_STATICCONTENT_H
#define LSST_QSERV_QHTTP_STATICCONTENT_H

// System headers
#include <string>

// Local headers
#include "qhttp/Server.h"

namespace lsst {
namespace qserv {
namespace qhttp {

class StaticContent
{
public:

    static void add(Server& server, std::string const& path, std::string const& rootDirectory);

};

}}} // namespace lsst::qserv::qhttp

#endif // LSST_QSER_QHTTP_STATICCONTENT_H
