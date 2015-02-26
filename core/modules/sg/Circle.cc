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

/// \file
/// \brief This file contains the Circle class implementation.

#include "Circle.h"

#include <ostream>

#include "Box.h"
#include "ConvexPolygon.h"
#include "Ellipse.h"


namespace lsst {
namespace sg {

double Circle::squaredChordLengthFor(Angle a) {
    if (a.asRadians() < 0.0) {
        return -1.0;
    }
    if (a.asRadians() >= PI) {
        return 4.0;
    }
    double s = sin(0.5 * a);
    return 4.0 * s * s;
}

Angle Circle::openingAngleFor(double squaredChordLength) {
    // Note: the maximum error in the opening angle (and circle bounding box
    // width) computations is ~ 2 * MAX_ASIN_ERROR.
    if (squaredChordLength < 0.0) {
        return Angle(-1.0);
    }
    if (squaredChordLength >= 4.0) {
        return Angle(PI);
    }
    return Angle(2.0 * std::asin(0.5 * std::sqrt(squaredChordLength)));
}

bool Circle::contains(Circle const & x) const {
    if (isFull() || x.isEmpty()) {
        return true;
    }
    if (isEmpty() || x.isFull()) {
        return false;
    }
    NormalizedAngle cc(_center, x._center);
    return _openingAngle >
           cc + x._openingAngle + 4.0 * Angle(MAX_ASIN_ERROR);
}

bool Circle::isDisjointFrom(Circle const & x) const {
    if (isEmpty() || x.isEmpty()) {
        return true;
    }
    if (isFull() || x.isFull()) {
        return false;
    }
    NormalizedAngle cc(_center, x._center);
    return cc > _openingAngle + x._openingAngle +
                4.0 * Angle(MAX_ASIN_ERROR);
}

Circle & Circle::clipTo(UnitVector3d const & x) {
    *this = contains(x) ? Circle(x) : empty();
    return *this;
}

Circle & Circle::clipTo(Circle const & x) {
    if (isEmpty() || x.isFull()) {
        return *this;
    }
    if (isFull() || x.isEmpty()) {
        *this = x;
        return *this;
    }
    Angle a = _openingAngle;
    Angle b = x._openingAngle;
    NormalizedAngle cc(_center, x._center);
    if (cc > a + b + 4.0 * Angle(MAX_ASIN_ERROR)) {
        // This circle is disjoint from x.
        *this = empty();
        return *this;
    }
    // The circles (nearly) intersect, or one contains the other.
    // For now, take the easy route and just use the smaller of
    // the two circles as a bound on their intersection.
    //
    // TODO(smm): Compute the minimal bounding circle.
    if (b < a) {
        *this = x;
    }
    return *this;
}

Circle & Circle::expandTo(UnitVector3d const & x) {
    // For any circle c and unit vector x, c.expandTo(x).contains(x)
    // should return true.
    if (isEmpty()) {
        *this = Circle(x);
    } else if (!contains(x)) {
        // Compute the normal vector for the plane defined by _center and x.
        UnitVector3d n = UnitVector3d::orthogonalTo(_center, x);
        // The minimal bounding circle (MBC) includes unit vectors on the plane
        // with normal n that span from _center.rotatedAround(n, -_openingAngle)
        // to x. The MBC center is the midpoint of this interval.
        NormalizedAngle cx(_center, x);
        Angle o = 0.5 * (cx + _openingAngle);
        Angle r = 0.5 * (cx - _openingAngle);
        // Rotate _center by angle r around n to obtain the MBC center. This is
        // done using Rodriques' formula, simplified by taking advantage of the
        // orthogonality of _center and n.
        _center = UnitVector3d(_center * cos(r) + n.cross(_center) * sin(r));
        _squaredChordLength = squaredChordLengthFor(o + Angle(MAX_ASIN_ERROR));
        _openingAngle = o + Angle(MAX_ASIN_ERROR);
    }
    return *this;
}

Circle & Circle::expandTo(Circle const & x) {
    if (isEmpty() || x.isFull()) {
        *this = x;
        return *this;
    }
    if (x.isEmpty() || isFull()) {
        return *this;
    }
    NormalizedAngle cc(_center, x._center);
    if (cc + x._openingAngle + 4.0 * Angle(MAX_ASIN_ERROR) <= _openingAngle) {
        // This circle contains x.
        return *this;
    }
    if (cc + _openingAngle + 4.0 * Angle(MAX_ASIN_ERROR) <= x._openingAngle) {
        // x contains this circle.
        *this = x;
        return *this;
    }
    // The circles intersect or are disjoint.
    Angle o = 0.5 * (cc + _openingAngle + x._openingAngle);
    if (o + 2.0 * Angle(MAX_ASIN_ERROR) >= Angle(PI)) {
        *this = full();
        return *this;
    }
    // Compute the normal vector for the plane defined by the circle centers.
    UnitVector3d n = UnitVector3d::orthogonalTo(_center, x._center);
    // The minimal bounding circle (MBC) includes unit vectors on the plane
    // with normal n that span from _center.rotatedAround(n, -_openingAngle)
    // to x._center.rotatedAround(n, x._openingAngle). The MBC center is the
    // midpoint of this interval.
    Angle r = o - _openingAngle;
    // Rotate _center by angle r around n to obtain the MBC center. This is
    // done using Rodriques' formula, simplified by taking advantage of the
    // orthogonality of _center and n.
    _center = UnitVector3d(_center * cos(r) + n.cross(_center) * sin(r));
    _squaredChordLength = squaredChordLengthFor(o + Angle(MAX_ASIN_ERROR));
    _openingAngle = o + Angle(MAX_ASIN_ERROR);
    return *this;
}

Circle & Circle::dilateBy(Angle r) {
    if (!isEmpty() && !isFull() &&
        (r.asRadians() > 0.0 || r.asRadians() < 0.0)) {
        Angle o = _openingAngle + r;
        _squaredChordLength = squaredChordLengthFor(o);
        _openingAngle = o;
    }
    return *this;
}

Circle & Circle::complement() {
    if (isEmpty()) {
        // The complement of an empty circle is a full circle.
        _squaredChordLength = 4.0;
        _openingAngle = Angle(PI);
    } else if (isFull()) {
        // The complement of a full circle is an empty circle.
        _squaredChordLength = -1.0;
        _openingAngle = Angle(-1.0);
    } else {
        _center = -_center;
        _squaredChordLength = 4.0 - _squaredChordLength;
        _openingAngle = Angle(PI) - _openingAngle;
    }
    return *this;
}

Box Circle::getBoundingBox() const {
    LonLat c(_center);
    Angle h = _openingAngle + 2.0 * Angle(MAX_ASIN_ERROR);
    NormalizedAngle w(Box::halfWidthForCircle(h, c.getLat()) +
                      Angle(MAX_ASIN_ERROR));
    return Box(c, w, h);
}

int Circle::relate(UnitVector3d const & v) const {
    if (contains(v)) {
        return CONTAINS | INTERSECTS;
    } else if (isEmpty()) {
        return DISJOINT | WITHIN;
    }
    return DISJOINT;
}

int Circle::relate(Box const & b) const {
    // Box-Circle relations are implemented by Box.
    return invertSpatialRelations(b.relate(*this));
}

int Circle::relate(Circle const & c) const {
    if (isEmpty()) {
        if (c.isEmpty()) {
            return CONTAINS | DISJOINT | WITHIN;
        }
        return DISJOINT | WITHIN;
    } else if (c.isEmpty()) {
        return CONTAINS | DISJOINT;
    }
    // Neither circle is empty.
    if (isFull()) {
        if (c.isFull()) {
            return CONTAINS | INTERSECTS | WITHIN;
        }
        return CONTAINS | INTERSECTS;
    } else if (c.isFull()) {
        return INTERSECTS | WITHIN;
    }
    // Neither circle is full.
    NormalizedAngle cc(_center, c._center);
    if (cc > _openingAngle + c._openingAngle + 4.0 * Angle(MAX_ASIN_ERROR)) {
        return DISJOINT;
    }
    int rel = INTERSECTS;
    if (cc + c._openingAngle + 4.0 * Angle(MAX_ASIN_ERROR) <= _openingAngle) {
        rel |= CONTAINS;
    } else if (cc + _openingAngle + 4.0 * Angle(MAX_ASIN_ERROR) <=
               c._openingAngle) {
        rel |= WITHIN;
    }
    return rel;
}

int Circle::relate(ConvexPolygon const & p) const {
    // ConvexPolygon-Circle relations are implemented by ConvexPolygon.
    return invertSpatialRelations(p.relate(*this));
}

int Circle::relate(Ellipse const & e) const {
    // Ellipse-Circle relations are implemented by Ellipse.
    return invertSpatialRelations(e.relate(*this));
}

std::ostream & operator<<(std::ostream & os, Circle const & c) {
    return os << "Circle(" << c.getCenter() << ", "
              << c.getSquaredChordLength() << ')';
}

}} // namespace lsst::sg
