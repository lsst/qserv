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

#ifndef LSST_SG_NORMALIZEDANGLEINTERVAL_H_
#define LSST_SG_NORMALIZEDANGLEINTERVAL_H_

/// \file
/// \brief This file declares a class representing closed intervals of
///        normalized angles, i.e. intervals of the unit circle.

#include <iosfwd>

#include "NormalizedAngle.h"


namespace lsst {
namespace sg {

/// `NormalizedAngleInterval` represents closed intervals of normalized angles,
/// i.e. intervals of the unit circle.
///
/// A point on the unit circle is represented by the angle ∈ [0, 2π) between
/// it and a reference point, and an interval by a pair of bounding points
/// a and b. The points in the interval are traced out by counter-clockwise
/// rotation of a around the circle until it reaches b. Because the endpoints
/// are represented via normalized angles, a can be greater than b, indicating
/// that the interval consists of the points represented by angles
/// [a, 2π) ⋃ [0, b]. When this is the case, calling `wraps()` on the interval
/// will return true.
///
/// An interval with identical endpoints contains just that point, and is equal
/// to that point.
///
/// An interval with NaN as either endpoint is empty, meaning that it contains
/// no points on the unit circle. The interval [0, 2 * PI] is full, meaning
/// that it contains all representable normalized angles in [0, 2π).
class NormalizedAngleInterval {
public:
    // Factory functions
    static NormalizedAngleInterval fromDegrees(double a, double b) {
        return NormalizedAngleInterval(Angle::fromDegrees(a),
                                       Angle::fromDegrees(b));
    }

    static NormalizedAngleInterval fromRadians(double a, double b) {
        return NormalizedAngleInterval(Angle(a), Angle(b));
    }

    static NormalizedAngleInterval empty() {
        return NormalizedAngleInterval();
    }

    static NormalizedAngleInterval full() {
        return NormalizedAngleInterval(NormalizedAngle(0.0),
                                       NormalizedAngle(2.0 * PI));
    }

    /// This constructor creates an empty interval.
    NormalizedAngleInterval() :
        _a(NormalizedAngle::nan()), _b(NormalizedAngle::nan()) {}

    /// This constructor creates a closed interval containing only
    /// the normalization of x.
    explicit NormalizedAngleInterval(Angle const & x) : _a(x), _b(_a) {}

    /// This constructor creates a closed interval containing only x.
    explicit NormalizedAngleInterval(NormalizedAngle const & x) :
        _a(x), _b(x) {}

    /// This constructor creates an interval from the given endpoints.
    /// If both x and y lie in the range [0, 2π), then y may be less than x.
    /// For example, passing in x = 3π/2 and y = π/2 will result in an interval
    /// containing angles [3π/2, 2π) ⋃ [0, π/2]. Otherwise, x must be less than
    /// or equal to y, and the interval will correspond to the the set of
    /// angles produced by normalizing the elements of the interval [x, y].
    NormalizedAngleInterval(Angle x, Angle y);

    /// This constructor creates an interval with the given endpoints.
    NormalizedAngleInterval(NormalizedAngle x, NormalizedAngle y) :
        _a(x), _b(y) {}

    /// Two intervals are equal if they contain the same points.
    bool operator==(NormalizedAngleInterval const & i) const {
        return (_a == i._a && _b == i._b) || (isEmpty() && i.isEmpty());
    }

    bool operator!=(NormalizedAngleInterval const & i) const {
        return !(*this == i);
    }

    /// A closed interval is equal to a point x if both endpoints equal x.
    bool operator==(NormalizedAngle x) const {
        return (_a == x && _b == x) || (x.isNan() && isEmpty());
    }

    bool operator!=(NormalizedAngle x) const { return !(*this == x); }

    /// `getA` returns the first endpoint of this interval.
    NormalizedAngle getA() const { return _a; }

    /// `getB` returns the second endpoint of this interval.
    NormalizedAngle getB() const { return _b; }

    /// `isEmpty` returns true if this interval does not contain any
    /// normalized angles.
    bool isEmpty() const { return _a.isNan() || _b.isNan(); }

    /// `isFull` returns true if this interval contains all normalized angles.
    bool isFull() const {
        return _a.asRadians() == 0.0 && _b.asRadians() == 2.0 * PI;
    }

    /// `wraps` returns true if the interval "wraps" around the 0/2π
    /// angle discontinuity, i.e. consists of `[getA(), 2π) ∪ [0, getB()]`.
    bool wraps() const { return _a > _b; }

    /// `getCenter` returns the center of this interval. It is NaN for empty
    /// intervals, and arbitrary for full intervals.
    NormalizedAngle getCenter() const { return NormalizedAngle::center(_a, _b); }

    /// `getSize` returns the size (length, width) of this interval. It is zero
    /// for single-point intervals, and NaN for empty intervals. Note that due
    /// to rounding errors, an interval with size `2 * PI` is not necessarily
    /// full, and an interval with size 0 may contain more than a single point.
    NormalizedAngle getSize() const { return _a.getAngleTo(_b); }

    ///@{
    /// `contains` returns true if the intersection of this interval and x
    /// is equal to x.
    bool contains(NormalizedAngle x) const {
        if (x.isNan()) {
            return true;
        }
        return wraps() ? (x <= _b || _a <= x) : (_a <= x && x <= _b);
    }

    bool contains(NormalizedAngleInterval const & x) const;
    ///@}

    ///@{
    /// `isDisjointFrom` returns true if the intersection of this interval
    /// and x is empty.
    bool isDisjointFrom(NormalizedAngle x) const {
        return !intersects(x);
    }

    bool isDisjointFrom(NormalizedAngleInterval const & x) const;
    ///@}

    ///@{
    /// `intersects` returns true if the intersection of this interval and x
    /// is non-empty.
    bool intersects(NormalizedAngle x) const {
        return wraps() ? (x <= _b || _a <= x) : (_a <= x && x <= _b);
    }

    bool intersects(NormalizedAngleInterval const & x) const {
        return !isDisjointFrom(x);
    }
    ///@}

    ///@{
    /// `isWithin` returns true if the intersection of this interval and x
    /// is this interval.
    bool isWithin(NormalizedAngle x) const {
        return (_a == x && _b == x) || isEmpty();
    }

    bool isWithin(NormalizedAngleInterval const & x) const {
        return x.contains(*this);
    }
    ///@}

    ///@{
    /// `relate` returns a bit field R describing the spatial relations between
    /// this interval and x. For each relation that holds, the bitwise and of
    /// R and the corresponding `SpatialRelation` bit mask will be non-zero.
    int relate(NormalizedAngle x) const;
    int relate(NormalizedAngleInterval const & x) const;
    ///@}

    /// `clipTo` shrinks this interval until all its points are in x.
    NormalizedAngleInterval & clipTo(NormalizedAngle x) {
        *this = clippedTo(x);
        return *this;
    }

    /// `x.clipTo(y)` sets x to the smallest interval containing the
    /// intersection of x and y. The result is not always unique, and
    /// `x.clipTo(y)` is not guaranteed to equal `y.clipTo(x)`.
    NormalizedAngleInterval & clipTo(NormalizedAngleInterval const & x);

    /// `clippedTo` returns the intersection of this interval and x.
    NormalizedAngleInterval clippedTo(NormalizedAngle x) const {
        return contains(x) ? NormalizedAngleInterval(x) : empty();
    }

    /// `clippedTo` returns the smallest interval containing the intersection
    /// of this interval and x. The result is not always unique, and
    /// `x.clippedTo(y)` is not guaranteed to equal `y.clippedTo(x)`.
    NormalizedAngleInterval clippedTo(NormalizedAngleInterval const & x) const {
        return NormalizedAngleInterval(*this).clipTo(x);
    }

    ///@{
    /// `expandTo` minimally expands this interval to contain x. The result
    /// is not always unique, and `x.expandTo(y)` is not guaranteed to equal
    /// `y.expandTo(x)`.
    NormalizedAngleInterval & expandTo(NormalizedAngle x);
    NormalizedAngleInterval & expandTo(NormalizedAngleInterval const & x);
    ///@}

    ///@{
    /// `expandedTo` returns the smallest interval containing the union of
    /// this interval and x. The result is not always unique, and
    /// `x.expandedTo(y)` is not guaranteed to equal `y.expandedTo(x)`.
    NormalizedAngleInterval expandedTo(NormalizedAngle x) const {
        return NormalizedAngleInterval(*this).expandTo(x);
    }

    NormalizedAngleInterval expandedTo(NormalizedAngleInterval const & x) const {
        return NormalizedAngleInterval(*this).expandTo(x);
    }
    ///@}

    /// For positive x, `dilatedBy` returns the morphological dilation of this
    /// interval by [-x,x]. For negative x, it returns the morphological
    /// erosion of this interval by [x,-x]. If x is zero or NaN, or this
    /// interval is empty, there is no effect.
    NormalizedAngleInterval dilatedBy(Angle x) const;
    NormalizedAngleInterval erodedBy(Angle x) const { return dilatedBy(-x); }

    NormalizedAngleInterval & dilateBy(Angle x) {
        *this = dilatedBy(x);
        return *this;
    }

    NormalizedAngleInterval & erodeBy(Angle x) { return dilateBy(-x); }

private:
    NormalizedAngle _a;
    NormalizedAngle _b;
};

std::ostream & operator<<(std::ostream &, NormalizedAngleInterval const &);

}} // namespace lsst::sg

#endif // LSST_SG_NORMALIZEDANGLEINTERVAL_H_
