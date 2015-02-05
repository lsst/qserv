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

#ifndef LSST_SG_INTERVAL_H_
#define LSST_SG_INTERVAL_H_

/// \file
/// \brief This file defines a template representing closed real intervals.

#include <algorithm>
#include <ostream>

#include "SpatialRelation.h"


namespace lsst {
namespace sg {

/// `Interval` represents a closed interval of the real numbers
/// by its upper and lower bounds. It is parameterized by the Scalar
/// type, which must be constructible from a double.
///
/// An interval with identical upper and lower bounds contains
/// a single point, and is equal to that scalar bound.
///
/// An interval with an upper bound less than its lower bound is
/// empty (contains no points), as are intervals with NaN as
/// either bound.
template <typename Derived, typename Scalar>
class Interval {
public:
    /// This constructor creates an empty interval.
    Interval() : _a(1.0), _b(0.0) {}

    /// This constructor creates a closed interval containing only x.
    explicit Interval(Scalar x) : _a(x), _b(x) {}

    /// This constructor creates an interval from the given endpoints.
    Interval(Scalar x, Scalar y) : _a(x), _b(y) {}

    /// Two closed intervals are equal if their endpoints are the same, or
    /// both are empty.
    bool operator==(Interval const & i) const {
        return (_a == i._a && _b == i._b) || (i.isEmpty() && isEmpty());
    }

    bool operator!=(Interval const & i) const { return !(*this == i); }

    /// A closed interval is equal to a point x if both endpoints equal x.
    bool operator==(Scalar x) const {
        return (_a == x && _b == x) || (x != x && isEmpty());
    }

    bool operator!=(Scalar x) const { return !(*this == x); }

    /// `getA` returns the lower endpoint of this interval. The return value
    /// for empty intervals is arbitrary.
    Scalar getA() const { return _a; }

    /// `getB` returns the upper endpoint of this interval. The return value
    /// for empty intervals is arbitrary.
    Scalar getB() const { return _b; }

    /// `isEmpty` returns true if this interval does not contain any points.
    bool isEmpty() const {
        return !(_a <= _b); // returns true when _a and/or _b is NaN
    }

    /// `getCenter` returns the center of this interval. It is arbitrary
    /// for empty intervals.
    Scalar getCenter() const { return 0.5 * (_a + _b); }

    /// `getSize` returns the size (length, width) of this interval. It is zero
    /// for single-point intervals, and NaN or negative for empty intervals.
    Scalar getSize() const { return _b - _a; }

    ///@{
    /// `contains` returns true if the intersection of this interval and x
    /// is equal to x.
    bool contains(Scalar x) const {
        return (_a <= x && x <= _b) || x != x;
    }

    bool contains(Interval const & x) const {
        if (x.isEmpty()) {
            return true;
        } else if (isEmpty()) {
            return false;
        }
        return _a <= x._a && _b >= x._b;
    }
    ///@}

    ///@{
    /// `isDisjointFrom` returns true if the intersection of this interval
    /// and x is empty.
    bool isDisjointFrom(Scalar x) const {
        return !intersects(x);
    }

    bool isDisjointFrom(Interval const & x) const {
        if (isEmpty() || x.isEmpty()) {
            return true;
        }
        return _a > x._b || _b < x._a;
    }
    ///@}

    ///@{
    /// `intersects` returns true if the intersection of this interval and x
    /// is non-empty.
    bool intersects(Scalar x) const { return _a <= x && x <= _b; }

    bool intersects(Interval const & x) const {
        return !isDisjointFrom(x);
    }
    ///@}

    ///@{
    /// `isWithin` returns true if the intersection of this interval and x
    /// is this interval.
    bool isWithin(Scalar x) const {
        return (_a == x && _b == x) || isEmpty();
    }

    bool isWithin(Interval const & x) const {
        return x.contains(*this);
    }
    ///@}

    ///@{
    /// `relate` returns a bit field F describing the spatial relations
    /// between this interval and x. For each relation that holds, the bitwise
    /// AND of F and the corresponding `SpatialRelation` bit mask will be
    /// non-zero.
    int relate(Scalar x) const;
    int relate(Interval const & x) const;
    ///@}

    ///@{
    /// `clipTo` shrinks this interval until all its points are in x.
    Interval & clipTo(Scalar x) {
        if (x != x) {
            _a = x;
            _b = x;
        } else {
            _a = std::max(_a, x);
            _b = std::min(_b, x);
        }
        return *this;
    }

    Interval & clipTo(Interval const & x) {
        if (x.isEmpty()) {
            *this = x;
        } else if (!isEmpty()) {
            _a = std::max(_a, x._a);
            _b = std::min(_b, x._b);
        }
        return *this;
    }
    ///@}

    ///@{
    /// `clippedTo` returns the intersection of this interval and x.
    Derived clippedTo(Scalar x) const { return Interval(*this).clipTo(x); }

    Derived clippedTo(Interval const & x) const {
        return Interval(*this).clipTo(x);
    }
    ///@}

    ///@{
    /// `expandTo` minimally expands this interval to contain x.
    Interval & expandTo(Scalar x) {
        if (isEmpty()) {
            _a = x;
            _b = x;
        } else if (x < _a) {
            _a = x;
        } else if (x > _b) {
            _b = x;
        }
        return *this;
    }

    Interval & expandTo(Interval const & x) {
        if (isEmpty()) {
            *this = x;
        } else if (!x.isEmpty()) {
            _a = std::min(_a, x._a);
            _b = std::max(_b, x._b);
        }
        return *this;
    }
    ///@}

    ///@{
    /// `expandedTo` returns the smallest interval containing the union
    /// of this interval and x.
    Derived expandedTo(Scalar x) const { return Interval(*this).expandTo(x); }

    Derived expandedTo(Interval const & x) const {
        return Interval(*this).expandTo(x);
    }
    ///@}

    /// For positive x, `dilateBy` morphologically dilates this interval
    /// by [-x,x], which is equivalent to the taking the Minkowski sum with
    /// [-x,x]. For negative x, it morphologically erodes this interval by
    /// [x,-x]. If x is zero or NaN, or this interval is empty, there is no
    /// effect.
    Interval & dilateBy(Scalar x) {
        if (x == x && !isEmpty()) {
            _a = _a - x;
            _b = _b + x;
        }
        return *this;
    }

    Interval & erodeBy(Scalar x) { return dilateBy(-x); }
    Derived dilatedBy(Scalar x) const { return Interval(*this).dilateBy(x); }
    Derived erodedBy(Scalar x) const { return Interval(*this).erodeBy(x); }

private:
    Scalar _a;
    Scalar _b;
};


template <typename Derived, typename Scalar>
int Interval<Derived, Scalar>::relate(Scalar x) const {
    if (isEmpty()) {
        if (x != x) {
            return CONTAINS | DISJOINT | WITHIN;
        }
        return DISJOINT | WITHIN;
    }
    if (x != x) {
        return CONTAINS | DISJOINT;
    }
    if (_a == x && _b == x) {
        return CONTAINS | INTERSECTS | WITHIN;
    }
    if (intersects(x)) {
        return CONTAINS | INTERSECTS;
    }
    return DISJOINT;
}

template <typename Derived, typename Scalar>
int Interval<Derived, Scalar>::relate(
    Interval<Derived, Scalar> const & x) const
{
    if (isEmpty()) {
        if (x.isEmpty()) {
            return CONTAINS | DISJOINT | WITHIN;
        }
        return DISJOINT | WITHIN;
    }
    if (x.isEmpty()) {
        return CONTAINS | DISJOINT;
    }
    if (_a == x._a && _b == x._b) {
        return CONTAINS | INTERSECTS | WITHIN;
    }
    if (_a > x._b || _b < x._a) {
        return DISJOINT;
    }
    if (_a <= x._a && _b >= x._b) {
        return CONTAINS | INTERSECTS;
    }
    if (x._a <= _a && x._b >= _b) {
        return INTERSECTS | WITHIN;
    }
    return INTERSECTS;
}

template <typename Derived, typename Scalar>
std::ostream & operator<<(std::ostream & os,
                          Interval<Derived, Scalar> const & x)
{
    return os << '[' << x.getA() << ", " << x.getB() << ']';
}

}} // namespace lsst::sg

#endif // LSST_SG_INTERVAL_H_
