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

#ifndef LSST_SG_ELLIPSE_H_
#define LSST_SG_ELLIPSE_H_

/// \file
/// \brief This file declares a class for representing elliptical
///        regions on the unit sphere.

#include <iosfwd>

#include "Circle.h"
#include "Matrix3d.h"
#include "Region.h"
#include "UnitVector3d.h"


namespace lsst {
namespace sg {

/// `Ellipse` is an elliptical region on the sphere.
///
/// Mathematical Definition
/// -----------------------
///
/// A spherical ellipse is defined as the set of unit vectors v such that:
///
///     d(v,f₁) + d(v,f₂) ≤ 2α                           (Eq. 1)
///
/// where f₁ and f₂ are unit vectors corresponding to the foci of the ellipse,
/// d is the function that returns the angle between its two input vectors, and
/// α is a constant.
///
/// If 2α < d(f₁,f₂), no point in S² satisfies the inequality, and the ellipse
/// is empty. If f₁ = f₂, the ellipse is a circle with opening angle α. The
/// ellipse defined by foci -f₁ and -f₂, and angle π - α satisfies:
///
///     d(v,-f₁) + d(v,-f₂) ≤ 2(π - α)                 →
///     π - d(v,f₁) + π - d(v,f₂) ≤ 2π - 2α            →
///     d(v,f₁) + d(v,f₂) ≥ 2α
///
/// In other words, it is the closure of the complement of the ellipse defined
/// by f₁, f₂ and α. Therefore if 2π - 2α ≤ d(f₁,f₂), all points in S² satisfy
/// Eq 1. and we say that the ellipse is full.
///
/// Consider now the equation d(v,f₁) + d(v,f₂) = 2α for v ∈ ℝ³. We know that
///
///     cos(d(v,fᵢ)) = (v·fᵢ)/(‖v‖‖fᵢ‖)
///                  = (v·fᵢ)/‖v‖            (since ‖fᵢ‖ = 1)
///
/// and, because sin²θ + cos²θ = 1 and ‖v‖² = v·v,
///
///     sin(d(v,fᵢ)) = √(v·v - (v·fᵢ)²)/‖v‖
///
/// Starting with:
///
///     d(v,f₁) + d(v,f₂) = 2α
///
/// we take the cosine of both sides, apply the angle sum identity for cosine,
/// and substitute the expressions above to obtain:
///
///     cos(d(v,f₁) + d(v,f₂)) = cos 2α                                   →
///     cos(d(v,f₁)) cos(d(v,f₂)) - sin(d(v,f₁)) sin(d(v,f₂)) = cos 2α    →
///     (v·f₁) (v·f₂) - √(v·v - (v·f₁)²) √(v·v - (v·f₂)²) = cos 2α (v·v)
///
/// Rearranging to place the square roots on the RHS, squaring both sides,
/// and simplifying:
///
///     ((v·f₁) (v·f₂) - cos 2α (v·v))² = (v·v - (v·f₁)²) (v·v - (v·f₂)²) →
///     cos²2α (v·v) - 2 cos 2α (v·f₁) (v·f₂) = (v·v) - (v·f₁)² - (v·f₂)² →
///
///     sin²2α (v·v) + 2 cos 2α (v·f₁) (v·f₂) - (v·f₁)² - (v·f₂)² = 0   (Eq. 2)
///
/// Note in particular that if α = π/2, the above simplifies to:
///
///      (v·f₁ + v·f₂)² = 0    ↔    v·(f₁ + f₂) = 0
///
/// That is, the equation describes the great circle obtained by intersecting
/// S² with the plane having normal vector f₁ + f₂.
///
/// Writing v = (x, y, z) and substituting into Eq. 2, we see that the LHS
/// is a homogeneous polynomial of degree two in 3 variables, or a ternary
/// quadratic form. The matrix representation of this quadratic form is the
/// symmetric 3 by 3 matrix Q such that:
///
///     vᵀ Q v = 0
///
/// is equivalent to Eq. 2. Consider now the orthonormal basis vectors:
///
///     b₀ = (f₁ - f₂)/‖f₁ - f₂‖
///     b₁ = (f₁ × f₂)/‖f₁ × f₂‖
///     b₂ = (f₁ + f₂)/‖f₁ + f₂‖
///
/// where x denotes the vector cross product. Let S be the matrix with these
/// basis vectors as rows. Given coordinates u in this basis, we have
/// v = Sᵀ u, and:
///
///     (Sᵀ u)ᵀ Q (Sᵀ u) = 0    ↔    uᵀ (S Q Sᵀ) u = 0
///
/// We now show that D = S Q Sᵀ is diagonal. Let d(f₁,f₂) = 2ɣ. The coordinates
/// of f₁ and f₂ in this new basis are f₁ = (sin ɣ, 0, cos ɣ) and
/// f₂ = (-sin ɣ, 0, cos ɣ). Writing u = (x, y, z) and substituting into
/// Eq. 2:
///
///      sin²2α (u·u) + 2 cos 2α (u·f₁) (u·f₂) - (u·f₁)² - (u·f₂)² = 0
///
/// we obtain:
///
///      (sin²2α - 2 cos 2α sin²ɣ - 2 sin²ɣ) x² + (sin²2α) y² +
///      (sin²2α + 2 cos 2α cos²ɣ - 2 cos²ɣ) z² = 0
///
/// Now sin²2α = 4 sin²α cos²α, cos 2α = cos²α - sin²α, so that:
///
///      (cos²α (sin²α - sin²ɣ)) x² + (sin²α cos²α) y² +
///      (sin²α (cos²α - cos²ɣ)) z² = 0
///
/// Dividing by sin²α (cos²ɣ - cos²α), and letting cos β = cos α / cos ɣ:
///
///       x² cot²α + y² cot²β - z² = 0              (Eq. 3)
///
/// This says that the non-zero elements of S Q Sᵀ are on the diagonal and
/// equal to (cot²α, cot²β, -1) up to scale. In other words, the boundary
/// of a spherical ellipse is given by the intersection of S² and an elliptical
/// cone in ℝ³ passing through the origin. Because z = 0 → x,y = 0 it is evident
/// that the boundary of a spherical ellipse is hemispherical.
///
/// If 0 < α < π/2, then β ≤ α, and α is the semi-major axis angle of
/// the ellipse while β is the semi-minor axis angle.
///
/// If α = π/2, then the spherical ellipse corresponds to a hemisphere.
///
/// If π/2 < α < π, then β ≥ α, and α is the semi-minor axis angle of
/// the ellipse, while β is the semi-major axis angle.
///
/// Implementation
/// --------------
///
/// Internal state consists of the orthogonal transformation matrix S that maps
/// the ellipse center to (0, 0, 1), as well as |cot α| and |cot β| (enough to
/// reconstruct D, and hence Q), and α, β, ɣ.
///
/// In fact, a = α - π/2, b = β - π/2 are stored instead of α and β. This is
/// for two reasons. The first is that when taking the complement of an
/// ellipse, α is mapped to π - α but a is mapped to -a (and b → -b). As a
/// result, taking the complement can be implemented using only changes of
/// sign, and is therefore exact. The other reason is that |cot(α)| = |tan(a)|,
/// and tan is more convenient numerically. In particular, cot(0) is undefined,
/// but tan is finite since a is rational and cannot be exactly equal to ±π/2.
class Ellipse : public Region {
public:
    static Ellipse empty() { return Ellipse(); }

    static Ellipse full() { return Ellipse().complement(); }

    /// This constructor creates an empty ellipse.
    Ellipse() :
        _S(1.0),
        _a(-2.0),
        _b(-2.0),
        _gamma(0.0),
        _tana(std::numeric_limits<double>::infinity()),
        _tanb(std::numeric_limits<double>::infinity())
    {}

    /// This constructor creates an ellipse corresponding to the given circle.
    explicit Ellipse(Circle const & c) {
        *this = Ellipse(c.getCenter(), c.getCenter(), c.getOpeningAngle());
    }

    /// This constructor creates an ellipse corresponding to the circle with
    /// the given center and opening angle.
    explicit Ellipse(UnitVector3d const & v, Angle alpha = Angle(0.0)) {
        *this = Ellipse(v, v, alpha);
    }

    /// This constructor creates an ellipse with the given foci and semi-axis
    /// angle.
    Ellipse(UnitVector3d const & f1, UnitVector3d const & f2, Angle alpha);

    /// This constructor creates an ellipse with the given center, semi-axis
    /// angles, and orientation. The orientation is defined as the position
    /// angle (east of north) of the first axis with respect to the
    /// north pole. Note that both alpha and beta must be less than, greater
    /// than, or equal to PI/2.
    Ellipse(UnitVector3d const & center,
            Angle alpha,
            Angle beta,
            Angle orientation);

    bool operator==(Ellipse const & e) const {
        return _S == e._S && _a == e._a && _b == e._b;
    }

    bool operator!=(Ellipse const & e) const { return !(*this == e); }

    bool isEmpty() const { return Angle(0.5 * PI) + _a < _gamma; }

    bool isFull() const { return Angle(0.5 * PI) - _a <= _gamma; }

    bool isGreatCircle() const { return _a.asRadians() == 0.0; }

    bool isCircle() const { return _a == _b; }

    /// `getTransformMatrix` returns the orthogonal matrix that maps vectors
    /// to the basis in which the quadratic form corresponding to this ellipse
    /// is diagonal.
    Matrix3d const & getTransformMatrix() const { return _S; }

    /// `getCenter` returns the center of the ellipse as a unit vector.
    UnitVector3d getCenter() const {
        return UnitVector3d::fromNormalized(_S(2,0), _S(2,1), _S(2,2));
    }

    /// `getF1` returns the first focal point of the ellipse.
    UnitVector3d getF1() const {
        UnitVector3d n = UnitVector3d::fromNormalized(_S(1,0), _S(1,1), _S(1,2));
        return getCenter().rotatedAround(n, -_gamma);
    }

    /// `getF2` returns the second focal point of the ellipse.
    UnitVector3d getF2() const {
        UnitVector3d n = UnitVector3d::fromNormalized(_S(1,0), _S(1,1), _S(1,2));
        return getCenter().rotatedAround(n, _gamma);
    }

    /// `getAlpha` returns α, the first semi-axis length of the ellipse. It is
    /// negative for empty ellipses, ≥ π for full ellipses and in [0, π)
    /// otherwise.
    Angle getAlpha() const { return Angle(0.5 * PI) + _a; }

    /// `getBeta` returns β, the second semi-axis length of the ellipse. It is
    /// negative for empty ellipses, ≥ π for full ellipses and in [0, π)
    /// otherwise.
    Angle getBeta() const { return Angle(0.5 * PI) + _b; }

    /// `getGamma` returns ɣ ∈ [0, π/2], half of the angle between the foci. The
    /// return value is arbitrary for empty and full ellipses.
    Angle getGamma() const { return _gamma; }

    /// `complement` sets this ellipse to the closure of its complement.
    Ellipse & complement() {
        _S(0,0) = -_S(0,0); _S(0,1) = -_S(0,1); _S(0,2) = -_S(0,2);
        _S(2,0) = -_S(2,0); _S(2,1) = -_S(2,1); _S(2,2) = -_S(2,2);
        _a = -_a;
        _b = -_b;
        return *this;
    }

    /// `complemented` returns the closure of the complement of this ellipse.
    Ellipse complemented() const { return Ellipse(*this).complement(); }

    // Region interface
    virtual Ellipse * clone() const { return new Ellipse(*this); }

    virtual Box getBoundingBox() const;

    virtual Circle getBoundingCircle() const;

    virtual bool contains(UnitVector3d const &v) const;

    virtual int relate(Region const & r) const {
        // Dispatch on the type of r.
        return invertSpatialRelations(r.relate(*this));
    }

    virtual int relate(Box const &) const;
    virtual int relate(Circle const &) const;
    virtual int relate(ConvexPolygon const &) const;
    virtual int relate(Ellipse const &) const;

private:
    Matrix3d _S;
    Angle _a; // α - π/2
    Angle _b; // β - π/2
    Angle _gamma; // Half the angle between the ellipse foci
    double _tana; // |tan a| = |cot α|
    double _tanb; // |tan b| = |cot β|
};

std::ostream & operator<<(std::ostream &, Ellipse const &);

}} // namespace lsst::sg

#endif // LSST_SG_ELLIPSE_H_
