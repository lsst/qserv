/*
 * LSST Data Management System
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
#include "global/stringUtil.h"

// System headers
#include <algorithm>
#include <fstream>
#include <limits>
#include <stdexcept>

// Third-party headers
#include <boost/algorithm/string/trim.hpp>

namespace lsst::qserv {

unsigned int stoui(std::string const& str, size_t* idx, int base) {
    unsigned long u = std::stoul(str, idx, base);
    if (u > std::numeric_limits<unsigned int>::max()) throw std::out_of_range(str);
    return static_cast<unsigned int>(u);
}

std::string interpolateFile(std::string_view str, std::filesystem::path const& basePath) {
    std::string s(str);
    std::string const filePrefix = "file:";
    if (s.substr(0, filePrefix.size()) == filePrefix) {
        std::filesystem::path refPath(s.substr(filePrefix.size()));
        if (!refPath.is_absolute()) refPath = basePath / refPath;
        std::ifstream f(refPath);
        if (!f) throw std::runtime_error("stringUtil: cannot open file '" + refPath.string() + "'");
        s.assign(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
        boost::trim(s);
    }
    return s;
}

}  // namespace lsst::qserv
