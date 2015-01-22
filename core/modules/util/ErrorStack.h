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

typedef std::pair<int, std::string> IntStringError;

/*
std::ostream& operator<<(std::ostream &out,
        IntStringError const& intStringError) {
    out << "    [" << intStringError.first << "] " << intStringError.second;
    return out;
}
*/

template<typename Error> class ErrorStack;

template<typename Error>
std::ostream& operator<<(std::ostream &out,
        ErrorStack<Error> const& errorContainer);

template<typename Error>
class ErrorStack: public std::exception {
public:
    void push(Error const&);
    std::string toString() const;
    virtual ~ErrorStack() throw () {
    }

    friend std::ostream& operator<<<Error>(std::ostream &out,
            ErrorStack<Error> const& errorContainer);

private:
    std::vector<Error> _errors;
};

}
}
} // namespace lsst::qserv::util

template class lsst::qserv::util::ErrorStack<std::pair<int, std::string> > ;

#endif /* UTIL_ERRORCONTAINER_H_ */
