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

    //----- StaticContent is a specialized Handler to handle the common case of serving a tree of static
    //      content rooted beneath a single file system directory.  add() will add an instance to the
    //      specified Server which will responding to GET requests on URL's that prefix match the pattern
    //      specified in the "path" argument and postfix match paths to existing files under rootDirectory
    //      in the local filesystem.  Content-Type of responses is inferred from the file extension for
    //      several common file extensions (see the file type map near the top of Response.cc for a complete
    //      list of these.)  Note that the Server::addStaticContent() convenience method would typically be
    //      called in preference to calling the add() method here directly.

    static void add(Server& server, std::string const& path, std::string const& rootDirectory);

};

}}} // namespace lsst::qserv::qhttp

#endif // LSST_QSER_QHTTP_STATICCONTENT_H
