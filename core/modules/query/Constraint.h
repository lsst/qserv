// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2015 LSST Corporation.
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
#ifndef LSST_QSERV_QUERY_CONSTRAINT_H
#define LSST_QSERV_QUERY_CONSTRAINT_H
/**
  * @file
  *
  * @brief Value class for query constraint
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <iosfwd>
#include <string>
#include <vector>

// Third-party headers
#include <memory>

namespace lsst {
namespace qserv {
namespace query {

/// A detected qserv constraint for C++ to Python
class Constraint {
public:
    std::string name;
    std::vector<std::string> params;
    std::string paramsGet(int i) const {
        return params[i];
    }
    int paramsSize() const {
        return params.size();
    }
};

std::ostream&
operator<<(std::ostream& os, Constraint const& c);
typedef std::vector<Constraint> ConstraintVector;

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_CONSTRAINT_H
