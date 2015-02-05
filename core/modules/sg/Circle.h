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

#ifndef LSST_SG_CIRCLE_H_
#define LSST_SG_CIRCLE_H_

/// \file
/// \brief This file declares a class for representing circular
///        regions on the unit sphere.

#include <iosfwd>

#include "Region.h"
#include "SpatialRelation.h"
#include "UnitVector3d.h"


namespace lsst {
namespace sg {

/// `Circle` is a circular region on the unit sphere that contains its
/// boundary. Internally, the circle is represented by its center vector
/// and the squared length of the chords between its center and points on
/// its boundary. This yields a fast point-in-circle test but, unlike a
/// representation that uses the center vector and cosine of the circle
/// opening angle, remains accurate for circles with very small opening
/// angles.
class Circle : public Region {
public:
    static Circle empty() { return Circle(); }

    static Circle full() { return Circle(UnitVector3d::Z(), 4.0); }

    /// `squaredChordLengthFor` computes and returns the squared chord length
    /// between points in S² that are separated by the given angle. The squared
    /// chord length l² and angle θ are related by l² = 4 sin²(θ/2).
    static double squaredChordLengthFor(Angle openingAngle);

    /// `openingAngleFor` computes and returns the angular separation between
    /// points in S² that are separated by the given squared chord length. The
    /// squared chord length l² and angle θ are related by l² = 4 sin²(θ/2).
    static Angle openingAngleFor(double squaredChordLength);

    /// This constructor creates an empty circle.
    Circle() :
        _center(UnitVector3d::Z()),
        _squaredChordLength(-1.0),
        _openingAngle(-1.0)
    {}

    /// This constructor creates the circle with center c and squared chord
    /// length / opening angle of zero. Because of rounding error,
    /// (v - c).squaredNorm() == 0.0 does not imply that v == c. Therefore
    /// calling contains(v) on the resulting circle may return true for unit
    /// vectors v != c.
    explicit Circle(UnitVector3d const & c) :
        _center(c),
        _squaredChordLength(0.0),
        _openingAngle(0.0)
    {}

    /// This constructor creates a circle with center c and opening angle a.
    /// If a is negative or NaN, the circle will be empty, and if a is greater
    /// than or equal to PI, the circle will be full.
    Circle(UnitVector3d const & c, Angle a) :
        _center(c),
        _squaredChordLength(squaredChordLengthFor(a)),
        _openingAngle(a)
    {}

    /// This constructor creates a circle with center c and squared chord
    /// length cl2. If cl2 is negative or NaN, the circle will be empty, and
    /// if cl2 is greater than or equal to 4, the circle will be full.
    Circle(UnitVector3d const & c, double cl2) :
        _center(c),
        _squaredChordLength(cl2),
        _openingAngle(openingAngleFor(cl2))
    {}

    bool operator==(Circle const & c) const {
        return (isEmpty() && c.isEmpty()) ||
               (isFull() && c.isFull()) ||
               (_center == c._center &&
                _squaredChordLength == c._squaredChordLength &&
                _openingAngle == c._openingAngle);
    }
    bool operator!=(Circle const & c) const { return !(*this == c); }

    bool isEmpty() const {
        // Return true when _squaredChordLength is negative or NaN.
        return !(_squaredChordLength >= 0.0);
    }

    bool isFull() const { return _squaredChordLength >= 4.0; }

    /// `getCenter` returns the center of this circle as a unit vector.
    /// It is arbitrary for empty and full circles.
    UnitVector3d const & getCenter() const { return _center; }

    /// `getSquaredChordLength` returns the squared length of chords between
    /// the circle center and points on the circle boundary. It is negative
    /// or NaN for empty circles, and at least 4 for full circles.
    double getSquaredChordLength() const { return _squaredChordLength; }

    /// `getOpeningAngle` returns the opening angle of this circle - that is,
    /// the angle between its center vector and points on its boundary. It
    /// is negative or NaN for empty circles, and at least PI for full circles.
    Angle getOpeningAngle() const { return _openingAngle; }

    /// `contains` returns true if the intersection of this circle and x
    /// is equal to x.
    bool contains(Circle const & x) const;

    ///@{
    /// `isDisjointFrom` returns true if the intersection of this circle and x
    /// is empty.
    bool isDisjointFrom(UnitVector3d const & x) const { return !contains(x); }
    bool isDisjointFrom(Circle const & x) const;
    ///@}

    ///@{
    /// `intersects` returns true if the intersection of this circle and x
    /// is non-empty.
    bool intersects(UnitVector3d const & x) const { return contains(x); }
    bool intersects(Circle const & x) const { return !isDisjointFrom(x); }
    ///@}

    ///@{
    /// `isWithin` returns true if the intersection of this circle and x
    /// is this circle.
    bool isWithin(UnitVector3d const &) const { return isEmpty(); }
    bool isWithin(Circle const & x) const { return x.contains(*this); }
    ///@}

    ///@{
    /// `clipTo` sets this circle to the minimal bounding circle for the
    /// intersection of this circle and x.
    Circle & clipTo(UnitVector3d const & x);
    Circle & clipTo(Circle const & x);
    ///@}

    ///@{
    /// `clippedTo` returns the minimal bounding circle for the intersection
    /// of this circle and x.
    Circle clippedTo(UnitVector3d const & x) const {
        return Circle(*this).clipTo(x);
    }

    Circle clippedTo(Circle const & x) const {
        return Circle(*this).clipTo(x);
    }
    ///@}

    ///@{
    /// `expandTo` minimally expands this circle to contain x.
    Circle & expandTo(UnitVector3d const & x);
    Circle & expandTo(Circle const & x);
    ///@}

    ///@{
    /// `expandedTo` returns the minimal bounding circle for the union
    /// of this circle and x.
    Circle expandedTo(UnitVector3d const & x) const {
        return Circle(*this).expandTo(x);
    }

    Circle expandedTo(Circle const & x) const {
        return Circle(*this).expandTo(x);
    }
    ///@}

    /// If r is positive, `dilateBy` increases the opening angle of this
    /// circle to include all points within angle r of its boundary. If r is
    /// negative, it decreases the opening angle to exclude those points
    /// instead.
    ///
    /// If this circle is empty or full, or r is zero or NaN,
    /// there is no effect.
    Circle & dilateBy(Angle r);
    Circle dilatedBy(Angle r) const { return Circle(*this).dilateBy(r); }
    Circle & erodeBy(Angle r) { return dilateBy(-r); }
    Circle erodedBy(Angle r) const { return dilatedBy(-r); }

    /// `getArea` returns the area of this circle in steradians.
    double getArea() const {
        return PI * std::max(0.0, std::min(_squaredChordLength, 4.0));
    }

    /// `complement` sets this circle to the closure of its complement. Note
    /// that both the empty circle as well as all circles containing a single
    /// point are mapped to a full circle, so that taking the complement of a
    /// circle twice is not guaranteed to reproduce the original circle, even
    /// in the absence of rounding error.
    Circle & complement();

    /// `complemented` returns the closure of the complement of this circle.
    Circle complemented() const { return Circle(*this).complement(); }

    int relate(UnitVector3d const & v) const;

    // Region interface
    virtual Circle * clone() const { return new Circle(*this); }

    virtual Box getBoundingBox() const;

    virtual Circle getBoundingCircle() const { return *this; }

    virtual bool contains(UnitVector3d const & v) const {
        return isFull() ||
               (v - _center).getSquaredNorm() <= _squaredChordLength;
    }

    virtual int relate(Region const & r) const {
        // Dispatch on the type of r.
        return invertSpatialRelations(r.relate(*this));
    }

    virtual int relate(Box const &) const;
    virtual int relate(Circle const &) const;
    virtual int relate(ConvexPolygon const &) const;
    virtual int relate(Ellipse const &) const;

private:
    UnitVector3d _center;
    double _squaredChordLength;
    Angle _openingAngle;
};

std::ostream & operator<<(std::ostream &, Circle const &);

}} // namespace lsst::sg

#endif // LSST_SG_CIRCLE_H_
