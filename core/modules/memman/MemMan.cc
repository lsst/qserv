// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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

// Qserv Headers
#include "MemManNone.h"

namespace lsst {
namespace qserv {
namespace memman {

/******************************************************************************/
/*                                C r e a t e                                 */
/******************************************************************************/
  
MemMan *MemMan::create(unsigned long long maxBytes, std::string const &dbPath) {
    (void)dbPath;

    // As of now we simply return a memory manager that does nothing
    //
    return new MemManNone(maxBytes);
}
}}} // namespace lsst:qserv:memman

