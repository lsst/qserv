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
#ifndef LSST_QSERV_QCACHE_EXCEPTIONS_H
#define LSST_QSERV_QCACHE_EXCEPTIONS_H

/**
 * Exceptions.h declares a collection of exceptions thrown by the implementation
 * of the result management service.
 * @note See individual class documentation for more information.
 */

// System headers

#include <stdexcept>
#include <string>

// This header declarations
namespace lsst {
namespace qserv {
namespace qcache {

/**
 * Class PageOverflow represents an exception thrown at an attempt to add
 * a row to a page that has already reached its maximum capacity.
 */
class PageOverflow: public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

}}} // namespace lsst::qserv::qcache

#endif // LSST_QSERV_QCACHE_EXCEPTIONS_H
