// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
#ifndef LSST_QSERV_STRINGUTIL_H
#define LSST_QSERV_STRINGUTIL_H
/**
 * @brief  Misc. lightweight string manipulation.
 */

// System headers
#include <cctype>
#include <filesystem>
#include <sstream>
#include <string>

namespace lsst::qserv {

/// @return true if a string is safe enough to use as a name in our SQL dialect.
bool inline isNameSafe(std::string::value_type const& c) {
    std::locale loc;
    if (std::isalnum(c)) {  // Not sure that using the default locale is safe.
        return true;
    }
    switch (c) {  // Special cases. '_' is the only one right now.
        case '_':
            return true;
        default:
            return false;
    }
}

/// Function object version of isNameSafe
struct isNameSafePred {
    inline bool operator()(std::string::value_type const& c) const { return isNameSafe(c); }
    typedef std::string::value_type argument_type;
};

/// @return a string equal to the original string with non-safe characters
/// removed.
inline std::string sanitizeName(std::string const& name) {
    std::string out;
    std::remove_copy_if(name.begin(), name.end(), std::insert_iterator<std::string>(out, out.begin()),
                        std::not1(isNameSafePred()));

    return out;
}

/// @return: string version of the contents of 'a'.
template <typename A>
std::string toString(A&& a) {
    //  boost::lexical_cast<std::string> is another option.
    std::ostringstream os;
    os << std::forward<A>(a);
    return os.str();
}

/**
 * Parse a string into an unsigned integer number.
 * @note The function that would complement the Standard Library's function std::stoi
 *   is not present in the Standard library. For unsigned integer types, the library
 *   only has std::stoul and std::stoull. Further details on this subject can be
 *   found in an official documentation for the latest C++ Standard.
 */
unsigned int stoui(std::string const& str, size_t* idx = 0, int base = 10);

/**
 * If the string starts with "file:", the rest of the string is treated as a file path
 * and the contents of the file are returned. If the path is relative, it is made
 * absolute by prepending basePath. If the string does not start with "file:", it is
 * returned unchanged.
 */
std::string interpolateFile(std::string_view str, std::filesystem::path const& basePath);

}  // namespace lsst::qserv

#endif  // LSST_QSERV_STRINGUTIL_H
