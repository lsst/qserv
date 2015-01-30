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
 * @brief Store a Qserv error
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

#ifndef UTIL_ERROR_H_
#define UTIL_ERROR_H_

// System headers
#include <string>

namespace lsst {
namespace qserv {
namespace util {

/** @class
 * @brief Store a Qserv error
 *
 * To be used with util::MultiError
 *
 */
class Error {
public:
    Error(int code, std::string const& msg) :
            code(code), msg(msg) {
    }
    virtual ~Error() {
    }
    std::string toString() const;

    int code;
    std::string msg;

    friend std::ostream& operator<<(std::ostream &out, Error const& error);
};

}}} // namespace lsst::qserv::util

#endif /* UTIL_ERROR_H_ */
