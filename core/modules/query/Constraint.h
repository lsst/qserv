/*
 * LSST Data Management System
 * Copyright 2009-2013 LSST Corporation.
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
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

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

/// A SWIG-purposed wrapper of a ConstraintVector.
class ConstraintVec {
public:
    ConstraintVec(boost::shared_ptr<ConstraintVector > v)
        : _vec(v) {}

    // SWIG-friendly interface
    Constraint const& get(int i) const {
        return (*_vec)[i];
    }
    int size() const {
        if(!_vec.get()) return 0; // NULL vector -> 0 size
        return _vec->size();
    }
    // Internal vector
    boost::shared_ptr<ConstraintVector> getVector() { return _vec; }

private:
    boost::shared_ptr<ConstraintVector> _vec;
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_CONSTRAINT_H
