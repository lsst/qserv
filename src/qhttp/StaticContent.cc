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

// Class header
#include "qhttp/StaticContent.h"

// Third-party headers
#include "boost/filesystem.hpp"
#include "boost/algorithm/string.hpp"

namespace fs = boost::filesystem;

namespace lsst {
namespace qserv {
namespace qhttp {


void StaticContent::add(
    Server& server,
    std::string const& pattern,
    std::string const& rootDirectory,
    boost::system::error_code& ec)
{
    fs::path rootPath = fs::canonical(rootDirectory, ec);
    if (ec) return;

    bool isDirectory = fs::is_directory(rootDirectory, ec);
    if (ec) return;

    if (!isDirectory) {
        ec = boost::system::errc::make_error_code(boost::system::errc::not_a_directory);
        return;
    }

    server.addHandler("GET", pattern, [rootPath](Request::Ptr request, Response::Ptr response) {

        // Don't let resource paths with embedded nulls past this point, since boost::filesystem does not
        // treat them consistently.  Assume we aren't intentionally serving any static content with embedded
        // nulls in the path, and just return a 404 if we find any.
        if (request->path.find('\0') != std::string::npos) {
            response->sendStatus(404);
            return;
        }

        // Defend against relative paths attempting to traverse above root directory.
        fs::path requestPath = rootPath;
        requestPath /= request->params["0"];
        requestPath = fs::weakly_canonical(requestPath);
        if (!boost::starts_with(requestPath, rootPath)) {
            response->sendStatus(403);
            return;
        }

        // Redirect directory paths without trailing "/", and default to "index.html" within directory paths.
        if (fs::is_directory(requestPath)) {
            if (!boost::ends_with(request->path, "/")) {
                response->headers["Location"] = request->path + "/";
                response->sendStatus(301);
                return;
            }
            requestPath /= "index.htm";
        }

        // Handle the oft-expected 404 case here explicitly, rather than as an exception.
        if (!fs::exists(requestPath)) {
            response->sendStatus(404);
            return;
        }

        // Other error cases are unexpected/unusual; Response::sendFile() can throw an exceptions to be caught
        // by top-level handler in Server::_dispatchRequest() if anything else goes wrong.
        response->sendFile(requestPath);
    });
}


}}} // namespace lsst::qserv::qhttp
