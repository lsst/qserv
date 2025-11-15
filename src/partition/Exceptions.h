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
#ifndef LSST_PARTITION_EXCEPTIONS_H
#define LSST_PARTITION_EXCEPTIONS_H

// System headers
#include <stdexcept>

// This header declarations
namespace lsst::partition {

/**
 * An exception type used to indicate that help information was requested.
 * The message associated with the exception contains the help text.
 */
class ExitOnHelp : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

}  // namespace lsst::partition

#endif  // LSST_PARTITION_EXCEPTIONS_H
