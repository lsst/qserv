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

#ifndef LSST_SG_UNITVECTOR3D_H_
#define LSST_SG_UNITVECTOR3D_H_

/// \file
/// \brief This file declares a class for representing unit vectors in ℝ³.

#include "LonLat.h"
#include "Vector3d.h"


namespace lsst {
namespace sg {

/// `UnitVector3d` is a unit vector in ℝ³ with components stored in double
/// precision.
///
/// Conceptually, a UnitVector3d is a Vector3d. However, it does not inherit
/// from Vector3d because all its mutators must correspond to isometries.
/// Propagating this restriction to the Vector3d base class would make the
/// base class unduly difficult to use. Furthermore, the base class assignment
/// operator would have to be made virtual - otherwise it would be possible to
/// cast a UnitVector3d reference to a Vector3d reference and then assign
/// components yielding non-unit norm. For a class this compact and performance
/// critical, the addition of a vtable pointer per instance and the potential
/// for virtual call overhead on assignment is deemed prohibitive.
class UnitVector3d {
public:
    /// `orthogonalTo` returns an arbitrary unit vector that is
    /// orthogonal to v.
    static UnitVector3d orthogonalTo(Vector3d const & v);

    /// `orthogonalTo` returns the unit vector n orthogonal to both v1 and v2
    /// and with the same direction as the cross product of v1 and v2. If v1
    /// and v2 are nearly (anti)parallel, then an arbitrary unit vector
    /// orthogonal to v1 is returned.
    static UnitVector3d orthogonalTo(Vector3d const & v1, Vector3d const & v2);

    /// `northFrom` returns the unit vector orthogonal to v that points "north"
    /// from v. More precisely, it returns a unit vector orthogonal to v that
    /// lies in the plane defined by v, the north pole (0, 0, 1), and the
    /// origin. If v is colinear with the z axis, then (-1, 0, 0) is returned
    /// if v points north, and (1, 0, 0) is returned if v points south.
    static UnitVector3d northFrom(Vector3d const & v);

    /// `orthogonalTo` returns the unit vector orthogonal to the meridian
    /// with the given longitude.
    static UnitVector3d orthogonalTo(NormalizedAngle const & a) {
        UnitVector3d u; u._v = Vector3d(-sin(a), cos(a), 0.0); return u;
    }

    /// `fromNormalized` returns the unit vector equal to v, which is assumed
    /// to be normalized. Use with caution - this assumption is not verified!
    static UnitVector3d fromNormalized(Vector3d const & v) {
        UnitVector3d u; u._v = v; return u;
    }

    /// `fromNormalized` returns the unit vector with the given components,
    /// which are assumed to correspond to those of a normalized vector. Use
    /// with caution - this assumption is not verified!
    static UnitVector3d fromNormalized(double x, double y, double z) {
        UnitVector3d u; u._v = Vector3d(x, y, z); return u;
    }

    static UnitVector3d X() {
        return UnitVector3d();
    }

    static UnitVector3d Y() {
        UnitVector3d u; u._v = Vector3d(0.0, 1.0, 0.0); return u;
    }

    static UnitVector3d Z() {
        UnitVector3d u; u._v = Vector3d(0.0, 0.0, 1.0); return u;
    }

    /// The default constructor creates a unit vector equal to (1, 0, 0).
    UnitVector3d() : _v(1.0, 0.0, 0.0) {}

    UnitVector3d(UnitVector3d const & v) : _v(v._v) {}

    ///@{
    /// This constructor creates a unit vector with the given direction.
    explicit UnitVector3d(Vector3d const & v) : _v(v) {
        _v.normalize();
    }

    UnitVector3d(double x, double y, double z) : _v(x, y, z) {
        _v.normalize();
    }
    ///@}

    /// This constructor creates the unit vector corresponding to the
    /// point `p` on the unit sphere.
    explicit UnitVector3d(LonLat const & p) {
        *this = UnitVector3d(p.getLon(), p.getLat());
    }

    /// This constructor creates a unit vector corresponding to
    /// the given spherical coordinates.
    UnitVector3d(Angle lon, Angle lat);

    /// This conversion operator returns a const reference to the underlying
    /// Vector3d. It allows a UnitVector3d to transparently replace a Vector3d
    /// as an argument in most function calls.
    operator Vector3d const & () const { return _v; }

    bool operator==(Vector3d const & v) const { return _v == v; }
    bool operator!=(Vector3d const & v) const { return _v != v; }

    /// `getData` returns a pointer to the 3 components of this unit vector.
    double const * getData() const { return _v.getData(); }

    /// The function call operator returns the `i`-th component of this vector.
    double operator()(int i) const { return _v(i); }

    /// `x` returns the first component of this unit vector.
    double x() const { return _v.x(); }

    /// `y` returns the second component of this unit vector.
    double y() const { return _v.y(); }

    /// `z` returns the third component of this unit vector.
    double z() const { return _v.z(); }

    /// `dot` returns the inner product of this unit vector and `v`.
    double dot(Vector3d const & v) const { return _v.dot(v); }

    /// `cross` returns the cross product of this unit vector and `v`.
    Vector3d cross(Vector3d const & v) const { return _v.cross(v); }

    /// `a.robustCross(b)` is `(b + a).cross(b - a)` - twice the cross
    /// product of a and b. The result is almost orthogonal to a and b
    /// even for nearly (anti-)parallel unit vectors. The idea comes from
    /// the Google S2 library.
    Vector3d robustCross(UnitVector3d const & v) const {
        return (v + *this).cross(v - *this);
    }

    /// The unary minus operator negates every component of this unit vector.
    UnitVector3d operator-() const {
        UnitVector3d u; u._v = -_v; return u;
    }

    /// The multiplication operator returns the component-wise product
    /// of this unit vector with scalar `s`.
    Vector3d operator*(double s) const { return _v * s; }

    /// The division operator returns the component-wise quotient
    /// of this unit vector with scalar `s`.
    Vector3d operator/(double s) const { return _v / s; }

    /// The addition operator returns the sum of this unit vector and `v`.
    Vector3d operator+(Vector3d const & v) const { return _v + v; }

    /// The subtraction operator returns the difference between
    /// this unit vector and `v`.
    Vector3d operator-(Vector3d const & v) const { return _v - v; }

    /// `cwiseProduct` returns the component-wise product of
    /// this unit vector and `v`.
    Vector3d cwiseProduct(Vector3d const & v) const {
        return _v.cwiseProduct(v);
    }

    /// `rotatedAround` returns a copy of this unit vector, rotated around the
    /// unit vector `k` by angle `a` according to the right hand rule.
    UnitVector3d rotatedAround(UnitVector3d const & k, Angle a) const {
        return UnitVector3d(_v.rotatedAround(k, a));
    }

private:
    Vector3d _v;
};


std::ostream & operator<<(std::ostream &, UnitVector3d const &);

}} // namespace lsst::sg

#endif // LSST_SG_UNITVECTOR3D_H_
