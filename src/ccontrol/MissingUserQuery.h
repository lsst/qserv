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
#ifndef LSST_QSERV_CCONTROL_MISSINGUSERQUERY_H
#define LSST_QSERV_CCONTROL_MISSINGUSERQUERY_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <exception>
#include <sstream>
#include <string.h>

// Third party headers
#include "boost/lexical_cast.hpp"

namespace lsst {
namespace qserv {
namespace ccontrol {

/// An error of trying to retrieve a non-existent UserQuery by id
class MissingUserQuery : public std::exception {
public:
    explicit MissingUserQuery(int id)
        : _s("Invalid UserQuery["
             + boost::lexical_cast<std::string>(id) +  "]") {
    }
    virtual ~MissingUserQuery() throw() {}
    virtual const char* what() const throw() { return _s.c_str(); }
private:
    std::string const _s;
};

}}} // namespace lsst::qserv::ccontrol

#endif // LSST_QSERV_CCONTROL_MISSINGUSERQUERY_H
