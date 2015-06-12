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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

#ifndef LSST_SG_SPATIALRELATION_H_
#define LSST_SG_SPATIALRELATION_H_

/// \file
/// \brief This file contains an enumeration of supported spatial relations.

namespace lsst {
namespace sg {

/// `SpatialRelation` enumerates the supported relations bewteen two spatial
/// objects. Because more than one relation can hold at once, the values are
/// powers of two that can be used as bitmasks.
enum SpatialRelation {
    /// A contains B  ⇔  A ⋂ B = B
    CONTAINS = 1,
    /// A is disjoint from B  ⇔  A ⋂ B = ∅
    DISJOINT = 2,
    /// A intersects B  ⇔  A ⋂ B ≠ ∅
    INTERSECTS = 4,
    /// A is within B  ⇔  A ⋂ B = A
    WITHIN = 8,
};

/// Given a bitfield describing the spatial relations between two regions
/// A and B (i.e. the output of `A.relate(B)`), `invertSpatialRelations`
/// returns the bitfield describing the relations between B and A
/// (`B.relate(A)`).
inline int invertSpatialRelations(int relations) {
    // The DISJOINT and INTERSECTS relations commute, so leave the
    // corresponding bits unchanged. If A CONTAINS B, then B is WITHIN A,
    // so the bits corresponding to CONTAINS and WITHIN must be swapped.
    return (relations & (DISJOINT | INTERSECTS)) |
           ((relations & CONTAINS) << 3) |
           ((relations & WITHIN) >> 3);
}

}} // namespace lsst::sg

#endif // LSST_SG_SPATIALRELATION_H_
