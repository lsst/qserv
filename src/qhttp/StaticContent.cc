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

// Local headers
#include "lsst/log/Log.h"
#include "qhttp/LogHelpers.h"
#include "qhttp/Status.h"

namespace errc = boost::system::errc;
namespace fs = boost::filesystem;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qhttp");
}

namespace lsst::qserv::qhttp {

void StaticContent::add(Server& server, std::string const& pattern, std::string const& rootDirectory) {
    fs::path rootPath;

    try {
        rootPath = fs::canonical(rootDirectory);  // may throw fs::filesystem_error
        if (!fs::is_directory(rootPath)) {
            throw fs::filesystem_error("boost::filesystem::is_directory", rootDirectory,
                                       errc::make_error_code(errc::not_a_directory));
        }
    }

    catch (fs::filesystem_error const& e) {
        // If anything unexpected happened, log here and rethrow
        LOGLS_ERROR(_log, logger(&server) << "failed adding static content: " << e.what());
        throw e;
    }

    server.addHandler("GET", pattern, [rootPath](Request::Ptr request, Response::Ptr response) {
        // Defend against relative paths attempting to traverse above root directory.
        fs::path requestPath = rootPath;
        requestPath /= request->params["0"];
        requestPath = fs::weakly_canonical(requestPath);
        if (!boost::starts_with(requestPath, rootPath)) {
            response->sendStatus(STATUS_FORBIDDEN);
            return;
        }

        // Redirect directory paths without trailing "/", and default to "index.html" within directory paths.
        // Note: according to boost documentation, fs:is_directory() returns false and does not throw on a
        // file not found condition; in that case we will fall through to the next block below.
        if (fs::is_directory(requestPath)) {
            if (!boost::ends_with(request->path, "/")) {
                response->headers["Location"] = request->path + "/";
                response->sendStatus(STATUS_MOVED_PERM);
                return;
            }
            requestPath /= "index.html";
        }

        // Handle the oft-expected case here explicitly, rather than as an exception.
        if (!fs::exists(requestPath)) {
            response->sendStatus(STATUS_NOT_FOUND);
            return;
        }

        // Other error cases are unexpected/unusual; Response::sendFile() can throw an exceptions to be caught
        // by top-level handler in Server::_dispatchRequest() if anything else goes wrong.
        response->sendFile(requestPath);
    });
}

}  // namespace lsst::qserv::qhttp
