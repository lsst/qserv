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

#ifndef LSST_SG_NORMALIZEDANGLE_H_
#define LSST_SG_NORMALIZEDANGLE_H_

/// \file
/// \brief This file declares a class for representing normalized angles.

#include "Angle.h"


namespace lsst {
namespace sg {

// Forward declarations
class LonLat;
class Vector3d;

/// `NormalizedAngle` is an angle that lies in the range [0, 2π), with one
/// exception - a NormalizedAngle can be NaN.
class NormalizedAngle {
public:
    static NormalizedAngle nan() {
        return NormalizedAngle(std::numeric_limits<double>::quiet_NaN());
    }

    static NormalizedAngle fromDegrees(double a) {
        return NormalizedAngle(a * RAD_PER_DEG);
    }

    static NormalizedAngle fromRadians(double a) {
        return NormalizedAngle(a);
    }

    /// For two angles a and b, `between(a, b)` returns the smaller of
    /// `a.getAngleTo(b)` and `b.getAngleTo(a)`. The result will be in the
    /// range [0, π].
    ///
    /// If one interprets an angle in [0, 2π) as a point on the unit circle,
    /// then `between` can be thought of as computing the arc length of the
    /// shortest unit circle segment between the points for a and b.
    static NormalizedAngle between(NormalizedAngle const & a,
                                   NormalizedAngle const & b);

    /// For two normalized angles a and b, `center(a, b)` returns the angle m
    /// such that `a.getAngleTo(m)` is equal to `m.getAngleTo(b)`.
    static NormalizedAngle center(NormalizedAngle const & a,
                                  NormalizedAngle const & b);

    /// This constructor creates a NormalizedAngle with a value of zero.
    NormalizedAngle() {}

    /// This constructor creates a copy of a.
    NormalizedAngle(NormalizedAngle const & a) : _a(a._a) {}

    /// This constructor creates a normalized copy of a.
    explicit NormalizedAngle(Angle const & a) {
        *this = NormalizedAngle(a.asRadians());
    }

    /// This constructor creates a NormalizedAngle with the given value in
    /// radians, normalized to be in the range [0, 2π).
    explicit NormalizedAngle(double a) {
        // For really large |a|, the error in this reduction can exceed 2*PI
        // (because PI is only an approximation to π).
        if (a < 0.0) {
            _a = Angle(std::fmod(a, 2.0 * PI) + 2.0 * PI);
        } else if (a > 2 * PI) {
            _a = Angle(std::fmod(a, 2.0 * PI));
        } else {
            _a = Angle(a);
        }
    }

    /// This constructor creates a NormalizedAngle equal to the angle between
    /// the given points on the unit sphere.
    NormalizedAngle(LonLat const &, LonLat const &);

    /// This constructor creates a NormalizedAngle equal to the angle between
    /// the given 3-vectors, which need not have unit norm.
    NormalizedAngle(Vector3d const &, Vector3d const &);

    /// This conversion operator returns a const reference to the underlying
    /// Angle. It allows a NormalizedAngle to transparently replace an Angle
    /// as an argument in most function calls.
    operator Angle const & () const { return _a; }

    // Comparison operators
    bool operator==(Angle const & a) const { return _a == a; }
    bool operator!=(Angle const & a) const { return _a != a; }
    bool operator<(Angle const & a) const { return _a < a; }
    bool operator>(Angle const & a) const { return _a > a; }
    bool operator<=(Angle const & a) const { return _a <= a; }
    bool operator>=(Angle const & a) const { return _a >= a; }

    // Arithmetic operators
    Angle operator-() const { return Angle(-_a); }
    Angle operator+(Angle const & a) const { return _a + a; }
    Angle operator-(Angle const & a) const { return _a - a; }
    Angle operator*(double a) const { return _a * a; }
    Angle operator/(double a) const { return _a / a; }
    double operator/(Angle a) const { return _a / a; }

    /// `asDegrees` returns the value of this angle in units of degrees.
    double asDegrees() const { return _a.asDegrees(); }

    /// `asRadians` returns the value of this angle in units of radians.
    double asRadians() const { return _a.asRadians(); }

    /// `isNan` returns true if the angle value is NaN.
    bool isNan() const { return _a.isNan(); }

    /// `getAngleTo` computes the angle α ∈ [0, 2π) such that adding α to this
    /// angle and then normalizing the result yields `a`.
    ///
    /// If one interprets an angle in [0, 2π) as a point on the unit circle,
    /// then this method can be thought of as computing the positive rotation
    /// angle required to map this point to `a`.
    NormalizedAngle getAngleTo(NormalizedAngle const & a) const {
        NormalizedAngle x;
        double d = a.asRadians() - asRadians();
        x._a = Angle((d < 0.0) ? 2.0 * PI + d : d);
        return x;
    }

private:
    Angle _a;
};

inline NormalizedAngle const & abs(NormalizedAngle const & a) { return a; }

}} // namespace lsst::sg

#endif // LSST_SG_NORMALIZEDANGLE_H_
