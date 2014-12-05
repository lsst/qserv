// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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
  * @brief SecondaryIndex implementation
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qproc/SecondaryIndex.h"

// System headersn

namespace lsst {
namespace qserv {
namespace qproc {

#if 0
       logger.inf("Looking for indexhints in ", hintList)
        secIndexSpecs = ifilter(lambda t: t[0] == "sIndex", hintList)
        lookups = []
        for s in secIndexSpecs:
            params = s[1]
// db table keycolumn, values
            lookup = IndexLookup(params[0], params[1], params[2], params[3:])
            lookups.append(lookup)
            pass
        index = SecondaryIndex()
        chunkIds = index.lookup(lookups)
        logger.inf("lookup got chunks:", chunkIds)
        return chunkIds
#endif

}}} // namespace lsst::qserv::qproc

