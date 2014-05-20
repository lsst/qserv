// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @brief Implementation of helper printer for Constraint
  *
  * @author Daniel L. Wang, SLAC
  */

#include "query/Constraint.h"

// System headers
#include <iterator>


namespace lsst {
namespace qserv {
namespace query {

std::ostream& operator<<(std::ostream& os, Constraint const& c) {
    os << "Constraint "
       << c.name << ": (";
    std::copy(c.params.begin(), c.params.end(),
              std::ostream_iterator<std::string>(os, ","));
    os << ")";
    return os;
}

}}} // namespace lsst::qserv::query

