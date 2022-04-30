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

// System headers
#include <memory>

// Qserv headers
#include "css/StripingParams.h"
#include "qproc/ChunkSpec.h"
#include "query/typedefs.h"

// forward declarations
namespace lsst::qserv::qproc {
class SecondaryIndex;
}  // namespace lsst::qserv::qproc

namespace lsst::qserv::qproc {

class IndexMap {
public:
    IndexMap(css::StripingParams const& sp, std::shared_ptr<SecondaryIndex> si);

    /** Compute the chunks list for the whole partitioning scheme
     *
     *  @returns all chunks of the partitioning scheme
     *
     */
    ChunkSpecVector getAllChunks();

    /**  Compute chunks coverage of spatial and secondary index restrictors
     *
     *   Index restrictors are combined with OR, and spatial restrictors are
     *   combined with OR, but the cumulative index restrictors are ANDed with
     *   the cumulative spatial restrictors.
     *
     *   @param areaRestrictors: Restrictors issued from SQL query.
     *   @param secIdxRestrictors: Restrictors issued for secondary index columns.
     *   @returns:  list of chunk queried by all secondary index search and spatial (i.e. UDF) restrictors.
     *
     *   FIXME: Index and spatial lookup composition is only supported using SQL "AND"
     *          operator for now. "OR" support has to be added, see DM-2888, DM-4017.
     */
    ChunkSpecVector getChunks(query::AreaRestrictorVecPtr const& areaRestrictors,
                              query::SecIdxRestrictorVecPtr const& secIdxRestrictors);

    class PartitioningMap;

private:
    std::shared_ptr<PartitioningMap> _pm;
    std::shared_ptr<SecondaryIndex> _si;
};

}  // namespace lsst::qserv::qproc
#endif  // LSST_QSERV_QPROC_INDEXMAP_H
