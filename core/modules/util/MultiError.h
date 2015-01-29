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

#ifndef LSST_QSERV_UTIL_ERROR_H
#define LSST_QSERV_UTIL_ERROR_H

// System headers
#include <exception>
#include <ostream>
#include <vector>

// Qserv headers
#include "util/Error.h"

namespace lsst {
namespace qserv {
namespace util {

/** @class
 * @brief Implement a generic error container for Qserv
 *
 * Store Qserv errors in a throwable vector.
 * util::Error operator << is used for output.
 *
 */
class MultiError: public std::exception {
public:
    std::string toString() const;
    virtual ~MultiError() throw () {
    }

    friend std::ostream& operator<<(std::ostream &out,
            MultiError const& multiError);

    bool empty() const;

    std::vector<Error>::size_type size() const;

    std::vector<Error>::const_iterator begin() const;

    std::vector<Error>::const_iterator end() const;

    std::vector<Error>::const_reference back() const;

    void push_back (const std::vector<Error>::value_type& val);

private:
    std::vector<Error> errorVector;

};

}}} // namespace lsst::qserv::util

#endif /* UTIL_ERRORCONTAINER_H_ */
