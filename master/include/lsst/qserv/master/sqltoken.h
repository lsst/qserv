// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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

#ifndef LSST_QSERV_MASTER_SQLTOKEN_H
#define LSST_QSERV_MASTER_SQLTOKEN_H
/**
  * @file parseTreeUtil.h
  *
  * @brief utility functions for working with SQL language tokens.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <boost/algorithm/string/predicate.hpp>

namespace lsst { namespace qserv { namespace master {

bool sqlShouldSeparate(std::string const& s, int last, int next);

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_XXXX_H

