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

#ifndef LSST_QSERV_QHTTP_PATH_H
#define LSST_QSERV_QHTTP_PATH_H

// System headers
#include <string>
#include <vector>

// Third-party headers
#include "boost/regex.hpp"

// Local headers
#include "qhttp/Request.h"

namespace lsst {
namespace qserv {
namespace qhttp {

//
// ----- This is an internal utility class, used by the Server class, that encapsulates compiling a path
//       specifier into a matching regexp, and then updating any captured params in a Request after matching
//       against the compiled regexp.  The internals of this are a fairly straight port of path-to-regexp
//       (https://github.com/pillarjs/path-to-regexp), as used by express.js; see that link for
//       examples of supported path syntax.
//

class Path
{
public:

    void parse(const std::string &pattern);
    void updateParamsFromMatch(Request::Ptr const& request, boost::smatch const& pathMatch);

    boost::regex regex;
    std::vector<std::string> paramNames;

};

}}} // namespace lsst::qserv::qhttp

#endif // LSST_QSERV_QHTTP_PATH_H
