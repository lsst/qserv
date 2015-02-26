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

#ifndef LSST_SG_REGION_H_
#define LSST_SG_REGION_H_

/// \file
/// \brief This file defines an interface for spherical regions.

namespace lsst {
namespace sg {

// Forward declarations
class Box;
class Circle;
class ConvexPolygon;
class Ellipse;
class UnitVector3d;

/// `Region` is a minimal interface for 2-dimensional regions on the unit
/// sphere. It provides two core pieces of functionality:
///
/// - It allows a region to be approximated with a simpler one.
/// - It allows for inexact computation of the spatial relationships
///   between two regions.
///
/// Given a partitioning of the unit sphere with partitions that can be
/// bounded by Regions, this provides all the necessary functionality for
/// determining which partitions may intersect a Region.
///
/// When implementing a new concrete region subclass R, the Region interface
/// should be extended with:
///
///     virtual int relate(R const &) const = 0;
///
/// All other Region subclasses must then implement this method. In
/// addition, R is expected to contain the following implementation of the
/// generic relate method:
///
///     virtual int relate(Region const & r) const {
///         return invertSpatialRelations(r.relate(*this));
///     }
///
/// where invertSpatialRelations is defined in SpatialRelations.h.
///
/// Given two Region references r1 and r2 of type R1 and R2, the net effect
/// is that r1.relate(r2) will end up calling R2::relate(R1 const &). In other
/// words, the call is polymorphic in the types of both r1 and r2.
///
/// One negative consequence of this design is that one cannot implement new
/// Region types outside of this library.
class Region {
public:
    virtual ~Region() {}

    /// `clone` returns a deep copy of this region.
    ///
    /// TODO(smm): Once Qserv moves to C++11, change this to return a
    ///            std::unique_ptr<Region> rather than a raw pointer.
    virtual Region * clone() const = 0;

    /// `getBoundingBox` returns a bounding-box for this region.
    virtual Box getBoundingBox() const = 0;

    /// `getBoundingCircle` returns a bounding-circle for this region.
    virtual Circle getBoundingCircle() const = 0;

    /// `contains` tests whether the given unit vector is inside this region.
    virtual bool contains(UnitVector3d const &) const = 0;

    ///@{
    /// `relate` computes the spatial relations between this region and another
    /// region R. The return value F is a bitfield with the following
    /// properties:
    ///
    /// - Bit `F & CONTAINS` is set only if this region contains all points in
    ///   R. If this is difficult/impossible to conclusively establish (e.g. due
    ///   to the imperfect nature of floating point arithmetic or the thorniness
    ///   of mathematics), or R contains a point not in this region, then the
    ///   bit shall remain unset.
    /// - Bit `F & INTERSECTS` is set if the intersection of this region and R
    ///   may be non-empty. Its value is the complement of `F & DISJOINT`.
    /// - Bit `F & WITHIN` is set only if R contains all points in this region.
    /// - Bit `F & DISJOINT` is set only if this region and R do not have any
    ///   points in common.
    ///
    /// Said another way: if the CONTAINS, WITHIN or DISJOINT bit is set, then
    /// the corresponding spatial relationship between the two regions holds
    /// conclusively. If it is not set, the relationship may or may not
    /// hold. Similarly, if the INTERSECTS bit is not set, the regions are
    /// conclusively disjoint. Otherwise, they may or may not intersect.
    ///
    /// These semantics allow for inexact spatial relation computations. In
    /// particular, a Region may choose to implement `relate` by replacing
    /// itself and/or the argument with a simplified bounding region.
    virtual int relate(Region const &) const = 0;
    virtual int relate(Box const &) const = 0;
    virtual int relate(Circle const &) const = 0;
    virtual int relate(ConvexPolygon const &) const = 0;
    virtual int relate(Ellipse const &) const = 0;
    ///@}
};

}} // namespace lsst::sg

#endif // LSST_SG_REGION_H_
