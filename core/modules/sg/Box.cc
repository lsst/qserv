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
/// \brief This file contains the Box class implementation.

#include "Box.h"

#include <cmath>
#include <ostream>

#include "Circle.h"
#include "ConvexPolygon.h"
#include "Ellipse.h"
#include "Utils.h"


namespace lsst {
namespace sg {

NormalizedAngle Box::halfWidthForCircle(Angle r, Angle lat) {
    if (r <= Angle(0.0)) {
        return NormalizedAngle(0.0);
    }
    // If a circle centered at the given latitude contains a pole, then
    // its bounding box contains all possible longitudes.
    if (abs(lat) + r >= Angle(0.5 * PI)) {
        return NormalizedAngle(PI);
    }
    // Now, consider the circle with opening angle r > 0 centered at (0,δ)
    // with r < π/2 and |δ| ≠ π/2. The circle center vector in ℝ³ is
    // c = (cos δ, 0, sin δ). Its bounding box spans longitudes [-α,α], where
    // α is the desired half-width. The plane corresponding to longitude α has
    // normal vector (-sin α, cos α, 0) and is tangent to the circle at point
    // p. The great circle segment between the center of the circle and the
    // plane normal passes through p and has arc length π/2 + r, so that
    //
    //    (cos δ, 0, sin δ) · (-sin α, cos α, 0) = cos (π/2 + r)
    //
    // Solving for α gives
    //
    //    α = arcsin (sin r / cos δ)
    //
    // In the actual computation, there is an absolute value and an explicit
    // arcsin domain check to cope with rounding errors. An alternate way to
    // compute this is:
    //
    //    α = arctan (sin r / √(cos(δ - r) cos(δ + r)))
    double s = std::fabs(sin(r) / cos(lat));
    if (s >= 1.0) {
        return NormalizedAngle(0.5 * PI);
    }
    return NormalizedAngle(std::asin(s));
}

Box & Box::dilateBy(Angle r) {
    // The basic idea is to compute the union of the bounding boxes for all
    // circles of opening angle r with centers inside this box.
    //
    // The bounding box for a circle of opening angle r with center latitude
    // |δ| ≤ π/2 - r has height 2r.
    //
    // Given fixed r, the width of the bounding box for the circle centered at
    // latitude δ grows monotonically with |δ| - for justification, see the
    // derivation in halfWidthForCircle(). The maximum width is therefore
    // attained when the circle is centered at one of the latitude angle
    // boundaries of this box. If max(|δ|) ≥ π/2 - r, it is 2π.
    //
    // Dilating the longitude interval of this box by the maximum width and
    // the latitude interval by r gives the desired result.
    if (isEmpty() || isFull() || r <= Angle(0.0)) {
        return *this;
    }
    Angle maxAbsLatitude = std::max(abs(_lat.getA()), abs(_lat.getB()));
    NormalizedAngle w = halfWidthForCircle(r, maxAbsLatitude);
    return dilateBy(w, r);
}

Box & Box::dilateBy(Angle w, Angle h) {
    if (isEmpty() || isFull()) {
        return *this;
    }
    _lon.dilateBy(w);
    if (!h.isNan()) {
        Angle a = (_lat.getA() > Angle(-0.5 * PI)) ? _lat.getA() - h : _lat.getA();
        Angle b = (_lat.getB() < Angle(0.5 * PI)) ? _lat.getB() + h : _lat.getB();
        _lat = AngleInterval(a, b);
    }
    _enforceInvariants();
    return *this;
}

double Box::getArea() const {
    if (isEmpty()) {
        return 0.0;
    }
    // Given a, b ∈ [-π/2, π/2] and a std::sin implementation that is not
    // correctly rounded, b > a does not imply that std::sin(b) > std::sin(a).
    // To avoid potentially returning a negative area, defensively take an
    // absolute value.
    double dz = sin(_lat.getB()) - sin(_lat.getA());
    return std::fabs(_lon.getSize().asRadians() * dz);
}

Circle Box::getBoundingCircle() const {
    if (isEmpty()) {
        return Circle::empty();
    }
    if (isFull()) {
        return Circle::full();
    }
    NormalizedAngle w = getWidth();
    // The minimal bounding circle center p lies on the meridian bisecting
    // this box. Let δ₁ and δ₂ be the minimum and maximum box latitudes.
    if (w.asRadians() <= PI) {
        UnitVector3d p;
        UnitVector3d boxVerts[4] = {
            UnitVector3d(_lon.getA(), _lat.getA()),
            UnitVector3d(_lon.getA(), _lat.getB()),
            UnitVector3d(_lon.getB(), _lat.getA()),
            UnitVector3d(_lon.getB(), _lat.getB())
        };
        // We take advantage of rotational symmetry to fix the bisecting
        // meridian at a longitude of zero. The box vertices then have
        // coordinates (±w/2, δ₁), (±w/2, δ₂), and p = (0, ϕ). Converting
        // to Cartesian coordinates gives p = (cos ϕ, 0, sin ϕ), and box
        // vertices at (cos w/2 cos δ₁, ±sin w/2 cos δ₁, sin δ₁) and
        // (cos w/2 cos δ₂, ±sin w/2 cos δ₂, sin δ₂).
        //
        // The point p₁ on the meridian that has minimum angular separation
        // to the vertices with latitude δ₁ lies on the plane they define.
        // The sum of the two vertex vectors is on that plane and on the plane
        // containing the meridian. Normalizing to obtain p₁, we have
        //
        //     (cos ϕ₁, 0, sin ϕ₁) =
        //         λ ((cos w/2 cos δ₁,  sin w/2 cos δ₁, sin δ₁) +
        //            (cos w/2 cos δ₁, -sin w/2 cos δ₁, sin δ₁))
        //
        // for some scaling factor λ. Simplifying, we get:
        //
        //     cos ϕ₁ = λ cos w/2 cos δ₁
        //     sin ϕ₁ = λ sin δ₁
        //
        // so that
        //
        //     tan ϕ₁ = sec w/2 tan δ₁
        //
        // Similarly, the point p₂ on the meridian that has minimum angular
        // separation to the vertices with latitude δ₂ satisfies:
        //
        //     tan ϕ₂ = sec w/2 tan δ₂
        //
        // where ϕ₁ ≤ ϕ₂ (since δ₁ ≤ δ₂). Finally, consider the point p₃
        // separated from each box vertex by the same angle. The dot
        // products of p₃ with the box vertices are all identical, so
        //
        //     cos ϕ₃ cos w/2 cos δ₁ + sin ϕ₃ sin δ₁ =
        //     cos ϕ₃ cos w/2 cos δ₂ + sin ϕ₃ sin δ₂
        //
        // Rearranging gives:
        //
        //     tan ϕ₃ = - cos w/2 (cos δ₁ - cos δ₂)/(sin δ₁ - sin δ₂)
        //
        // which can be simplified further using a tangent half-angle identity,
        // yielding:
        //
        //     tan ϕ₃ = cos w/2 tan (δ₁ + δ₂)/2
        //
        // Consider now the function f₁(ϕ) that gives the angular separation
        // between p with latitude ϕ and the vertices at latitude δ₁. It has
        // a line of symmetry at ϕ = ϕ₁, and increases monotonically with
        // |ϕ - ϕ₁|. Similarly, f₂(ϕ) has a minimum at ϕ₂ and increases
        // monotonically with |ϕ - ϕ₂|. The two functions cross at ϕ₃. The
        // opening angle of the bounding circle centered at latitude ϕ is
        // given by g = max(f₁, f₂), which we seek to minimize.
        //
        // If ϕ₁ ≤ ϕ₃ ≤ ϕ₂, then g is minimized at ϕ = ϕ₃. Otherwise, it
        // is minimized at either ϕ₁ or ϕ₂.
        double phi1, phi2, phi3;
        double c = cos(0.5 * w);
        if (c == 0.0) {
            // This code should never execute. If it does, the implementation
            // of std::cos is broken.
            phi1 = ::copysign(0.5 * PI, _lat.getA().asRadians());
            phi2 = ::copysign(0.5 * PI, _lat.getB().asRadians());
            phi3 = 0.0;
        } else {
            phi1 = std::atan(tan(_lat.getA()) / c);
            phi2 = std::atan(tan(_lat.getB()) / c);
            phi3 = std::atan(c * tan(_lat.getCenter()));
        }
        if (phi1 <= phi3 && phi3 <= phi2) {
            p = UnitVector3d(_lon.getCenter(), Angle(phi3));
        } else {
            UnitVector3d p1 = UnitVector3d(_lon.getCenter(), Angle(phi1));
            UnitVector3d p2 = UnitVector3d(_lon.getCenter(), Angle(phi2));
            if (p1.dot(boxVerts[0]) > p2.dot(boxVerts[1])) {
                p = p2;
            } else {
                p = p1;
            }
        }
        // Compute the maximum squared chord length between p and the box
        // vertices, so that each one is guaranteed to lie in the bounding
        // circle, regardless of numerical error in the above.
        double cl2 = (p - boxVerts[0]).getSquaredNorm();
        for (int i = 1; i < 4; ++i) {
            cl2 = std::max(cl2, (p - boxVerts[i]).getSquaredNorm());
        }
        // Add double the maximum squared-chord-length error, so that the
        // bounding circle we return also reliably CONTAINS this box.
        return Circle(p, cl2 + 2.0 * MAX_SCL_ERROR);
    }
    // The box spans more than π radians in longitude. First, pick the smaller
    // of the bounding circles centered at the north and south pole.
    Angle r;
    UnitVector3d v;
    if (abs(_lat.getA()) <= abs(_lat.getB())) {
        v = UnitVector3d::Z();
        r = Angle(0.5 * PI) - _lat.getA();
    } else {
        v = -UnitVector3d::Z();
        r = _lat.getB() + Angle(0.5 * PI);
    }
    // If the box does not span all longitude angles, we also consider the
    // equatorial bounding circle with center longitude equal to the longitude
    // of the box center. The smaller of the polar and equatorial bounding
    // circles is returned.
    if (!_lon.isFull() && 0.5 * w < r) {
        r = 0.5 * w;
        v = UnitVector3d(_lon.getCenter(), Angle(0.0));
    }
    return Circle(v, r + 4.0 * Angle(MAX_ASIN_ERROR));
}

int Box::relate(Circle const & c) const {
    if (isEmpty()) {
        if (c.isEmpty()) {
            return CONTAINS | DISJOINT | WITHIN;
        }
        return DISJOINT | WITHIN;
    } else if (c.isEmpty()) {
        return CONTAINS | DISJOINT;
    }
    if (isFull()) {
        if (c.isFull()) {
            return CONTAINS | INTERSECTS | WITHIN;
        }
        return CONTAINS | INTERSECTS;
    } else if (c.isFull()) {
        return INTERSECTS | WITHIN;
    }
    // Neither region is empty or full. We now determine whether or not the
    // circle and box boundaries intersect.
    //
    // If the box vertices are not all inside or all outside of c, then the
    // boundaries cross.
    LonLat vertLonLat[4] = {
        LonLat(_lon.getA(), _lat.getA()),
        LonLat(_lon.getA(), _lat.getB()),
        LonLat(_lon.getB(), _lat.getA()),
        LonLat(_lon.getB(), _lat.getB())
    };
    UnitVector3d verts[4];
    bool inside = false;
    for (int i = 0; i < 4; ++i) {
        verts[i] = UnitVector3d(vertLonLat[i]);
        double d = (verts[i] - c.getCenter()).getSquaredNorm();
        if (std::fabs(d - c.getSquaredChordLength()) < MAX_SCL_ERROR) {
            // A box vertex is close to the circle boundary.
            return INTERSECTS;
        }
        bool b = d < c.getSquaredChordLength();
        if (i == 0) {
            inside = b;
        } else if (inside != b) {
            // There are box vertices both inside and outside of c.
            return INTERSECTS;
        }
    }
    UnitVector3d norms[2] = {
        UnitVector3d::orthogonalTo(_lon.getA()),
        UnitVector3d::orthogonalTo(_lon.getB())
    };
    if (inside) {
        // All box vertices are inside c. Look for points in the box edge
        // interiors that are outside c.
        for (int i = 0; i < 2; ++i) {
            double d = getMaxSquaredChordLength(
                c.getCenter(), verts[2 * i + 1], verts[2 * i], norms[i]);
            if (d > c.getSquaredChordLength() - MAX_SCL_ERROR) {
                return INTERSECTS;
            }
        }
        LonLat cc(-c.getCenter());
        if (_lon.contains(cc.getLon())) {
            // The points furthest from the center of c on the small circles
            // defined by the box edges with constant latitude are in the box
            // edge interiors. Find the largest squared chord length to either.
            Angle a = std::min(getMinAngleToCircle(cc.getLat(), _lat.getA()),
                               getMinAngleToCircle(cc.getLat(), _lat.getB()));
            double d = Circle::squaredChordLengthFor(Angle(PI) - a);
            if (d > c.getSquaredChordLength() - MAX_SCL_ERROR) {
                return INTERSECTS;
            }
        }
        // The box boundary is completely inside c. However, the box is not
        // necessarily within c: consider a circle with opening angle equal to
        // π - ε. If a box contains the complement of such a circle, then
        // intersecting it with that circle will punch a hole in the box. In
        // this case each region contains the boundary of the other, but
        // neither region contains the other.
        //
        // To handle this case, check that the box does not contain the
        // complement of c - since the boundaries do not intersect, this is the
        // case iff the box contains the center of the complement of c.
        if (contains(cc)) {
            return INTERSECTS;
        }
        return INTERSECTS | WITHIN;
    }
    // All box vertices are outside c. Look for points in the box edge
    // interiors that are inside c.
    for (int i = 0; i < 2; ++i) {
        double d = getMinSquaredChordLength(
            c.getCenter(), verts[2 * i + 1], verts[2 * i], norms[i]);
        if (d < c.getSquaredChordLength() + MAX_SCL_ERROR) {
            return INTERSECTS;
        }
    }
    LonLat cc(c.getCenter());
    if (_lon.contains(cc.getLon())) {
        // The points closest to the center of c on the small circles
        // defined by the box edges with constant latitude are in the box
        // edge interiors. Find the smallest squared chord length to either.
        Angle a = std::min(getMinAngleToCircle(cc.getLat(), _lat.getA()),
                           getMinAngleToCircle(cc.getLat(), _lat.getB()));
        double d = Circle::squaredChordLengthFor(a);
        if (d < c.getSquaredChordLength() + MAX_SCL_ERROR) {
            return INTERSECTS;
        }
    }
    // The box boundary is completely outside of c. If the box contains the
    // circle center, then the box contains c. Otherwise, the box and circle
    // are disjoint.
    if (contains(cc)) {
        return CONTAINS | INTERSECTS;
    }
    return DISJOINT;
}

int Box::relate(ConvexPolygon const & p) const {
    // ConvexPolygon-Box relations are implemented by ConvexPolygon.
    return invertSpatialRelations(p.relate(*this));
}

int Box::relate(Ellipse const & e) const {
    // Ellipse-Box relations are implemented by Ellipse.
    return invertSpatialRelations(e.relate(*this));
}

std::ostream & operator<<(std::ostream & os, Box const & b) {
    return os << "Box(" << b.getLon() << ", " << b.getLat() << ')';
}

}} // namespace lsst::sg
