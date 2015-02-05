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

#ifndef LSST_SG_CONVEXPOLYGON_H_
#define LSST_SG_CONVEXPOLYGON_H_

/// \file
/// \brief This file declares a class for representing convex
///        polygons with great circle edges on the unit sphere.

#include <iosfwd>
#include <vector>

#include "Region.h"
#include "SpatialRelation.h"
#include "Vector3d.h"


namespace lsst {
namespace sg {

/// `ConvexPolygon` is a closed convex polygon on the unit sphere. Its edges
/// are great circles (geodesics), and the shorter of the two great circle
/// segments between any two points on the polygon boundary is contained in
/// the polygon.
///
/// The vertices of a convex polygon are distinct and have counter-clockwise
/// orientation when viewed from outside the unit sphere. No three consecutive
/// vertices are coplanar and edges do not intersect except at vertices.
///
/// Furthermore, if a convex polygon contains a point p of S², then we require
/// that it be disjoint from point -p. This guarantees the existence of a
/// unique shortest great circle segment between any 2 points contained in the
/// polygon, but means e.g. that hemispheres and lunes cannot be represented
/// by convex polygons.
///
/// Currently, the only way to construct a convex polygon is to compute the
/// convex hull of a point set.
class ConvexPolygon : public Region {
public:
    /// `convexHull` returns the convex hull of the given set of points if it
    /// exists and throws an exception otherwise. Though points are supplied
    /// in a vector, they really are conceptually a set - the ConvexPolygon
    /// returned is invariant under permutation of the input array.
    static ConvexPolygon convexHull(std::vector<UnitVector3d> const & points) {
        return ConvexPolygon(points);
    }

    /// This constructor creates a convex polygon that is the convex hull of
    /// the given set of points.
    ConvexPolygon(std::vector<UnitVector3d> const & points);

    /// Two convex polygons are equal iff they contain the same points.
    bool operator==(ConvexPolygon const & p) const;
    bool operator!=(ConvexPolygon const & p) const { return !(*this == p); }

    std::vector<UnitVector3d> const & getVertices() const {
        return _vertices;
    }

    /// The centroid of a polygon is its center of mass projected onto
    /// S², assuming a uniform mass distribution over the polygon surface.
    UnitVector3d getCentroid() const;

    // Region interface
    virtual ConvexPolygon * clone() const { return new ConvexPolygon(*this); }

    virtual Box getBoundingBox() const;
    virtual Circle getBoundingCircle() const;

    virtual bool contains(UnitVector3d const & v) const;

    virtual int relate(Region const & r) const {
        // Dispatch on the type of r.
        return invertSpatialRelations(r.relate(*this));
    }

    virtual int relate(Box const &) const;
    virtual int relate(Circle const &) const;
    virtual int relate(ConvexPolygon const &) const;
    virtual int relate(Ellipse const &) const;

private:
    typedef std::vector<UnitVector3d>::const_iterator VertexIterator;

    ConvexPolygon() : _vertices() {}

    std::vector<UnitVector3d> _vertices;
};

std::ostream & operator<<(std::ostream &, ConvexPolygon const &);

}} // namespace lsst::sg

#endif // LSST_SG_CONVEXPOLYGON_H_
