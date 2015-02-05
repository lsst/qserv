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

#ifndef LSST_SG_VECTOR3D_H_
#define LSST_SG_VECTOR3D_H_

/// \file
/// \brief This file declares a class for representing vectors in ℝ³.

#include <cmath>
#include <iosfwd>
#include <limits>
#include <stdexcept>


namespace lsst {
namespace sg {

// Forward declarations
class Angle;
class UnitVector3d;


/// `Vector3d` is a vector in ℝ³ with components stored in double precision.
class Vector3d {
public:
    /// The default constructor creates a zero vector.
    Vector3d() { _v[0] = 0.0; _v[1] = 0.0; _v[2] = 0.0; }

    /// This constructor creates a vector with the given components.
    Vector3d(double x, double y, double z) { _v[0] = x; _v[1] = y; _v[2] = z; }

    bool operator==(Vector3d const & v) const {
        return _v[0] == v._v[0] && _v[1] == v._v[1] && _v[2] == v._v[2];
    }

    bool operator!=(Vector3d const & v) const {
        return _v[0] != v._v[0] || _v[1] != v._v[1] || _v[2] != v._v[2];
    }

    ///@{
    /// `data` returns a pointer to the 3 components of this vector.
    double * getData() { return _v; }
    double const * getData() const { return _v; }
    ///@}

    ///@{
    /// The function call operator returns the `i`-th component of this vector.
    double & operator()(int i) { return _v[i]; }
    double operator()(int i) const { return _v[i]; }
    ///@}

    ///@{
    /// `x` returns the first component of this vector.
    double & x() { return _v[0]; }
    double x() const { return _v[0]; }
    ///@}

    ///@{
    /// `y` returns the second component of this vector.
    double & y() { return _v[1]; }
    double y() const { return _v[1]; }
    ///@}

    ///@{
    /// `z` returns the third component of this vector.
    double & z() { return _v[2]; }
    double z() const { return _v[2]; }
    ///@}

    /// `dot` returns the inner product of this vector and `v`.
    double dot(Vector3d const & v) const {
        return _v[0] * v._v[0] + _v[1] * v._v[1] + _v[2] * v._v[2];
    }
    /// `getSquaredNorm` returns the inner product of this vector with itself.
    double getSquaredNorm() const { return dot(*this); }
    /// `getNorm` returns the L2 norm of this vector.
    double getNorm() const {
        return std::sqrt(getSquaredNorm());
    }

    /// `isZero` returns true if all the components of this vector are zero.
    bool isZero() const { return *this == Vector3d(); }

    /// `normalize` scales this vector to have unit norm and returns its norm
    /// prior to scaling. It will accurately normalize any vector with finite
    /// components except for (0, 0, 0), including those with norms that
    /// overflow. Trying to normalize (0, 0, 0) will cause a std::runtime_error
    /// to be thrown.
    double normalize();

    /// `isNormalized` returns true if this vectors norm is very close to 1.
    bool isNormalized() const {
        return std::fabs(1.0 - getSquaredNorm()) <= 1e-15;
    }

    /// `cross` returns the cross product of this vector and `v`.
    Vector3d cross(Vector3d const & v) const {
        return Vector3d(_v[1] * v._v[2] - _v[2] * v._v[1],
                        _v[2] * v._v[0] - _v[0] * v._v[2],
                        _v[0] * v._v[1] - _v[1] * v._v[0]);
    }

    /// The unary minus operator negates every component of this vector.
    Vector3d operator-() const {
        return Vector3d(-_v[0],
                        -_v[1],
                        -_v[2]);
    }

    /// The multiplication operator returns the component-wise product
    /// of this vector with scalar `s`.
    Vector3d operator*(double s) const {
        return Vector3d(_v[0] * s,
                        _v[1] * s,
                        _v[2] * s);
    }

    /// The division operator returns the component-wise quotient
    /// of this vector with scalar `s`.
    Vector3d operator/(double s) const {
        return Vector3d(_v[0] / s,
                        _v[1] / s,
                        _v[2] / s);
    }

    /// The addition operator returns the sum of this vector and `v`.
    Vector3d operator+(Vector3d const & v) const {
        return Vector3d(_v[0] + v._v[0],
                        _v[1] + v._v[1],
                        _v[2] + v._v[2]);
    }

    /// The subtraction operator returns the difference between this vector and `v`.
    Vector3d operator-(Vector3d const & v) const {
        return Vector3d(_v[0] - v._v[0],
                        _v[1] - v._v[1],
                        _v[2] - v._v[2]);
    }

    Vector3d & operator*=(double s) { *this = *this * s; return *this; }
    Vector3d & operator/=(double s) { *this = *this / s; return *this; }
    Vector3d & operator+=(Vector3d const & v) { *this = *this + v; return *this; }
    Vector3d & operator-=(Vector3d const & v) { *this = *this - v; return *this; }

    /// `cwiseProduct` returns the component-wise product of this vector and `v`.
    Vector3d cwiseProduct(Vector3d const & v) const {
        return Vector3d(_v[0] * v._v[0],
                        _v[1] * v._v[1],
                        _v[2] * v._v[2]);
    }

    /// `rotatedAround` returns a copy of this vector, rotated around the
    /// unit vector k by angle a according to the right hand rule.
    Vector3d rotatedAround(UnitVector3d const & k, Angle a) const;

private:
    double _v[3];
};


inline Vector3d operator*(double s, Vector3d const & v) { return v * s; }

std::ostream & operator<<(std::ostream &, Vector3d const &);

}} // namespace lsst::sg

#endif // LSST_SG_VECTOR3D_H_
