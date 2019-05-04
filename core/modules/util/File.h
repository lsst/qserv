/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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
#ifndef LSST_QSERV_UTIL_FILE_H
#define LSST_QSERV_UTIL_FILE_H

// System headers
#include <string>
#include <vector>

// This header declarations

namespace lsst {
namespace qserv {
namespace util {

/**
 * Class File is a utility class providing convenient operations with files.
 */
class File {

public:
    
    // Instances of this class can't be constructed

    File() = delete;
    File(File const&) = delete;
    File & operator=(File const&) = delete;
    ~File() = delete;

    /**
     * Read the content of a file, line-by-line into a vector. Optionally test
     * of the result set is not empty.
     *
     * @param fileName        the name of a file
     * @param assertNotEmpty  (optional) flag which would trigger an exception if the file is empty
     *
     * @return a collection of lines read from the file
     *
     * @throws std::invalid_argument if the name of the file is empty
     * @throws std::runtime_error    if the file can't be open
     * @throws std::range_error      if the optional flag `assertNotEmpty` is set and the file is empty
     */
    /// 
    static std::vector<std::string> getLines(std::string const& fileName,
                                             bool assertNotEmpty=false);
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_FILE_H
