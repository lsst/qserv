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

#ifndef LSST_SG_ANGLE_H_
#define LSST_SG_ANGLE_H_

/// \file
/// \brief This file declares a class for representing angles.

#include <cmath>
#include <algorithm>
#include <iosfwd>
#include <limits>

#include "Constants.h"


namespace lsst {
namespace sg {

/// `Angle` represents an angle in radians. It provides methods for
/// angle comparison and arithmetic, as well as unit conversion.
/// An angle is said to be normalized if it lies in the range [0, 2π).
class Angle {
public:
    static Angle nan() {
        return Angle(std::numeric_limits<double>::quiet_NaN());
    }

    static Angle fromDegrees(double a) { return Angle(a * RAD_PER_DEG); }

    static Angle fromRadians(double a) { return Angle(a); }

    /// This constructor creates an Angle with a value of zero.
    Angle() : _rad(0.0) {}

    /// This constructor creates an Angle with the given value in radians.
    explicit Angle(double a) : _rad(a) {}

    // Comparison operators
    bool operator==(Angle const & a) const { return _rad == a._rad; }
    bool operator!=(Angle const & a) const { return _rad != a._rad; }
    bool operator<(Angle const & a) const { return _rad < a._rad; }
    bool operator>(Angle const & a) const { return _rad > a._rad; }
    bool operator<=(Angle const & a) const { return _rad <= a._rad; }
    bool operator>=(Angle const & a) const { return _rad >= a._rad; }

    // Arithmetic operators
    Angle operator-() const { return Angle(-_rad); }
    Angle operator+(Angle const & a) const { return Angle(_rad + a._rad); }
    Angle operator-(Angle const & a) const { return Angle(_rad - a._rad); }
    Angle operator*(double a) const { return Angle(_rad * a); }
    Angle operator/(double a) const { return Angle(_rad / a); }
    double operator/(Angle a) const { return _rad / a._rad; }

    // In-place arithmetic operators
    Angle & operator+=(Angle const & a) { *this = *this + a; return *this; }
    Angle & operator-=(Angle const & a) { *this = *this - a; return *this; }
    Angle & operator*=(double a) { *this = *this * a; return *this; }
    Angle & operator/=(double a) { *this = *this / a; return *this; }

    /// `asDegrees` returns the value of this angle in units of degrees.
    double asDegrees() const { return _rad * DEG_PER_RAD; }

    /// `asRadians` returns the value of this angle in units of radians.
    double asRadians() const { return _rad; }

    /// `isNormalized` returns true if this angle lies in the range [0, 2π).
    bool isNormalized() const { return _rad >= 0.0 && _rad <= 2.0 * PI; }

    /// `isNan` returns true if the angle value is NaN.
    bool isNan() const { return std::isnan(_rad); }

private:
    double _rad;
};


inline Angle operator*(double a, Angle const & b) { return b * a; }

std::ostream & operator<<(std::ostream &, Angle const &);

inline double sin(Angle const & a) { return std::sin(a.asRadians()); }
inline double cos(Angle const & a) { return std::cos(a.asRadians()); }
inline double tan(Angle const & a) { return std::tan(a.asRadians()); }

inline Angle abs(Angle const & a) { return Angle(std::fabs(a.asRadians())); }

}} // namespace lsst::sg

#endif // LSST_SG_ANGLE_H_
