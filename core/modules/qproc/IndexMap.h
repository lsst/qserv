// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Qserv headers
#include "css/StripingParams.h"
#include "query/Constraint.h"
#include "qproc/ChunkSpec.h"

namespace lsst {
namespace qserv {
namespace qproc {

class SecondaryIndex;

class IndexMap {
public:
    IndexMap(css::StripingParams const& sp,
             boost::shared_ptr<SecondaryIndex> si);

    ChunkSpecVector getAll();

    /// Index constraints are combined with OR, and spatial constraints are
    /// combined with OR, but the cumulative index constraints are ANDed with
    /// the cumulative spatial constraints.
    ChunkSpecVector getIntersect(query::ConstraintVector const& cv);

    class PartitioningMap;
private:
    boost::shared_ptr<PartitioningMap> _pm;
    boost::shared_ptr<SecondaryIndex> _si;
};

}}} // namespace lsst::qserv::qproc
#endif // LSST_QSERV_QPROC_INDEXMAP_H
