// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2020 LSST.
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

#ifndef LSST_QSERV_UTIL_STRINGHELPER_H
#define LSST_QSERV_UTIL_STRINGHELPER_H

// System headers
#include <string>
#include <vector>

// Qserv headers
#include "util/Command.h"

namespace lsst { namespace qserv { namespace util {

/// Functions to help with string processing.
class StringHelper {
public:
    /// @return a vector of strings resulting from splitting 'str' into separate strings
    /// using 'separator' as the delimiter.
    /// TODO: If other return types are needed, make a template version.
    static std::vector<std::string> splitString(std::string const& str, std::string const& separator);

    /// @return a vector of int resulting from splitting 'str' into separate strings
    /// and converting those strings into integers.
    /// Throws invalid_argument if throwOnError is true and one of the strings fails conversion.
    /// If throwOnError is false, the default value will be used for that entry and nothing will be thrown.
    /// TODO: If other return types are needed, make a template version.
    static std::vector<int> getIntVectFromStr(std::string const& str, std::string const& separator,
                                              bool throwOnError = true, int defaultVal = 0);
};

}}}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_STRINGHELPER_H
