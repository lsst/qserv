// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_UTIL_ERRORCONTAINER_H
#define LSST_QSERV_UTIL_ERRORCONTAINER_H

// System headers
#include <exception>
#include <ostream>
#include <vector>

namespace lsst {
namespace qserv {
namespace util {

template <typename T> class ErrorContainer: public std::exception {
public:
    typedef std::pair<int, std::string> IntString;
    typedef std::vector<IntString> IntStringVector;

    ErrorContainer();
    virtual ~ErrorContainer();

    void push(T const&);

    std::string toString() const;

    friend std::ostream& operator<<(std::ostream &out, ErrorContainer<T> const& errorContainer);

private:
    std::vector<T> _errors;
};

}
}
} // namespace lsst::qserv::util

#endif /* UTIL_ERRORCONTAINER_H_ */
