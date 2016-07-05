// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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

// system headers
#include <sys/resource.h>
#include <sys/time.h>

// Qserv Headers
#include "memman/MemMan.h"
#include "memman/MemManNone.h"
#include "memman/MemManReal.h"

/******************************************************************************/
/*                        G l o b a l   S t a t i c s                         */
/******************************************************************************/
  
namespace {

uint64_t memLockLimit() {
    rlim_t theMax;
    struct rlimit rlim;

    // Set our arbitrary limit when working with inifinity
    //
    theMax = (sizeof(theMax) > 4 ? 0x7fffffffffffffffULL : 0xffffffff);

    // Get the lock limit and if the soft limit is lower than the hard limit
    // set it to the hard limit. If the hard limit is infinity, set it to
    // some magically large number.
    //
    if (!getrlimit(RLIMIT_MEMLOCK, &rlim)) {
        if (rlim.rlim_max == RLIM_INFINITY) {
            rlim.rlim_cur = theMax;
            setrlimit(RLIMIT_MEMLOCK, &rlim);
            return theMax;
        } else {
            if (rlim.rlim_cur != rlim.rlim_max) {
                rlim.rlim_cur = rlim.rlim_max;
                setrlimit(RLIMIT_MEMLOCK, &rlim);
                return rlim.rlim_cur;
            } else theMax = rlim.rlim_cur;
        }
    }
    return theMax;
}
}

uint64_t lsst::qserv::memman::MemMan::lockLimit = memLockLimit();

/******************************************************************************/
/*                                M e m M a n                                 */
/******************************************************************************/
  
namespace lsst {
namespace qserv {
namespace memman {

/******************************************************************************/
/*                                C r e a t e                                 */
/******************************************************************************/
  
MemMan *MemMan::create(uint64_t maxBytes, std::string const &dbPath) {

    // Return a memory manager implementation
    //
    return new MemManReal(dbPath, maxBytes);
}
}}} // namespace lsst:qserv:memman

