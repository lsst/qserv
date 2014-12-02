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
#ifndef LSST_QSERV_QPROC_INDEXMAP_H
#define LSST_QSERV_QPROC_INDEXMAP_H
/**
  * @file
  *
  * @brief IndexMap to look up chunk numbers
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
// Temporary
#include "qproc/fakeGeometry.h"

namespace lsst {
namespace qserv {
namespace qproc {

boost::shared_ptr<Region> getRegion(Constraint const& c);

class IndexMap {
public:
    IndexMap(boost::shared_ptr<PartitioningMap> pm) {}
    void applyConstraints(boost::shared_ptr<query::ConstraintVector> cv) {
        
    }
    

};



}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_INDEXMAP_H
