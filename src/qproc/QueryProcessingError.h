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
#ifndef LSST_QSERV_QPROC_QUERYPROCESSINGERROR_H
#define LSST_QSERV_QPROC_QUERYPROCESSINGERROR_H

// System headers
#include <stdexcept>

namespace lsst {
namespace qserv {
namespace qproc {

/// QueryProcessingError marks an runtime error in query processing.
class QueryProcessingError : public std::runtime_error {
public:
    explicit QueryProcessingError(char const* msg)
        : std::runtime_error(msg) {}
    explicit QueryProcessingError(std::string const& msg)
        : std::runtime_error(msg) {}
};
}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_QUERYPROCESSINGERROR_H
