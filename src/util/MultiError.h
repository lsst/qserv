// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
/**
 * @file
 *
 * @ingroup util
 *
 * @brief Implement a generic error container for Qserv
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

#ifndef LSST_QSERV_UTIL_MULTIERROR_H
#define LSST_QSERV_UTIL_MULTIERROR_H

// System headers
#include <exception>
#include <map>
#include <ostream>
#include <vector>

// Qserv headers
#include "util/Error.h"

namespace lsst::qserv::util {

/// Implement a generic error container for Qserv
/// Errors are stored in a map using a code/subCode pair for the key.
/// The first error with a given code/subCode sets the message and
///  duplicate values increase the count of the error.
/// The hope is that numerous duplicate errors will just have a high
///    count and not obscure other important messages.
class MultiError {
public:
    MultiError() = default;
    MultiError(MultiError const& multiErr) = default;

    virtual ~MultiError() = default;

    bool operator==(MultiError const& other) const = default;

    /// Return a minimalistic string representation of the object
    /// @return a string representation of the object
    std::string toOneLineString() const;

    /// Return the error with the lowest error code.
    util::Error firstError() const;

    bool empty() const;

    std::vector<Error>::size_type size() const;

    std::vector<Error> getVector() const;

    /// Errors should set the error code to anything but NONE (0).
    /// The Error subCode may be any value, including NONE.
    void insert(Error const& val);
    void merge(MultiError const& other);

    //// Return a string representation of the object
    std::string toString() const;

    friend std::ostream& operator<<(std::ostream& out, MultiError const& multiError);

private:
    std::map<std::pair<int, int>, Error> _errorMap;
};

}  // namespace lsst::qserv::util

#endif /* LSST_QSERV_UTIL_MULTIERROR_H */
