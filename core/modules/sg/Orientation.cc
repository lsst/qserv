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
/// \brief This file contains the implementation of orientation().

#include "Orientation.h"

#include <algorithm>

#include "BigInteger.h"


namespace lsst {
namespace sg {

namespace {

// `BigFloat` is an exact floating point type.
struct BigFloat {
    BigInteger * mantissa;
    int exponent;

    BigFloat() : mantissa(0), exponent(0) {}
    BigFloat(BigInteger * m) : mantissa(m), exponent(0) {}

    bool operator<(BigFloat const & f) const {
        return exponent > f.exponent;
    }
};

// `computeProduct` computes the product of 3 doubles exactly and stores the
// result in p.
void computeProduct(BigFloat & p, double d0, double d1, double d2) {
    // This constant (2^53) is used to scale the fractions returned by
    // std::frexp and turn them into integer mantissas.
    static double const SCALE = 9007199254740992.0;
    uint32_t buf[2];
    BigInteger i(buf, sizeof(buf) / sizeof(uint32_t));
    // Unpack the 3 input doubles into integer mantissas and exponents.
    int e0 = 0;
    int e1 = 0;
    int e2 = 0;
    double m0 = std::frexp(d0, &e0) * SCALE;
    double m1 = std::frexp(d1, &e1) * SCALE;
    double m2 = std::frexp(d2, &e2) * SCALE;
    // Compute the product of the 3 input doubles using exact arithmetic.
    p.mantissa->setTo(static_cast<int64_t>(m0));
    i.setTo(static_cast<int64_t>(m1));
    p.mantissa->multiply(i);
    i.setTo(static_cast<int64_t>(m2));
    p.mantissa->multiply(i);
    // Finally, adjust the exponent of the result to compensate for the 3
    // multiplications by 2^53 performed above.
    p.exponent = e0 + e1 + e2 - 3 * 53;
}

} // unnamed namespace


int orientationExact(Vector3d const & a,
                     Vector3d const & b,
                     Vector3d const & c)
{
    // Product mantissa storage buffers.
    uint32_t mantissaBuffers[6][6];
    // Product mantissas.
    BigInteger mantissas[6] = {
        BigInteger(mantissaBuffers[0], 6),
        BigInteger(mantissaBuffers[1], 6),
        BigInteger(mantissaBuffers[2], 6),
        BigInteger(mantissaBuffers[3], 6),
        BigInteger(mantissaBuffers[4], 6),
        BigInteger(mantissaBuffers[5], 6)
    };
    BigFloat products[6] = {
        BigFloat(&mantissas[0]),
        BigFloat(&mantissas[1]),
        BigFloat(&mantissas[2]),
        BigFloat(&mantissas[3]),
        BigFloat(&mantissas[4]),
        BigFloat(&mantissas[5])
    };
    // An accumulator and its storage.
    uint32_t accumulatorBuffer[512];
    BigInteger accumulator(accumulatorBuffer,
                           sizeof(accumulatorBuffer) / sizeof(uint32_t));
    // Compute the products in the determinant. Performing all multiplication
    // up front means that each product mantissa occupies at most 3*53 bits.
    computeProduct(products[0], a.x(), b.y(), c.z());
    computeProduct(products[1], a.x(), b.z(), c.y());
    computeProduct(products[2], a.y(), b.z(), c.x());
    computeProduct(products[3], a.y(), b.x(), c.z());
    computeProduct(products[4], a.z(), b.x(), c.y());
    computeProduct(products[5], a.z(), b.y(), c.x());
    mantissas[1].negate();
    mantissas[3].negate();
    mantissas[5].negate();
    // Sort the array of products by decreasing exponent.
    std::sort(products, products + 6);
    // First, initialize the accumulator to the product with the highest
    // exponent, then add the remaining products. Prior to each addition, we
    // must shift the accumulated value so that its radix point lines up with
    // the the radix point of the product to add.
    //
    // More precisely, at each step we have an accumulated value A·2ʲ and a
    // product P·2ᵏ, and we update the accumulator to equal (A·2ʲ⁻ᵏ + P)·2ᵏ.
    // Because the products were sorted beforehand, j ≥ k and 2ʲ⁻ᵏ is an
    // integer.
    accumulator = *products[0].mantissa;
    for (int i = 1; i < 6; ++i) {
        accumulator.multiplyPow2(products[i - 1].exponent - products[i].exponent);
        accumulator.add(*products[i].mantissa);
    }
    return accumulator.getSign();
}

int orientation(UnitVector3d const & a,
                UnitVector3d const & b,
                UnitVector3d const & c)
{
    // This constant is a little more than 5ε, where ε = 2^-53. When multiplied
    // by the permanent of |M|, it gives an error bound on the determinant of
    // M. Here, M is a 3x3 matrix and |M| denotes the matrix obtained by
    // taking the absolute value of each of its components. The derivation of
    // this proceeds in the same manner as the derivation of the error bounds
    // in section 4.3 of:
    //
    //     Adaptive Precision Floating-Point Arithmetic
    //     and Fast Robust Geometric Predicates,
    //     Jonathan Richard Shewchuk,
    //     Discrete & Computational Geometry 18(3):305–363, October 1997.
    //
    // available online at http://www.cs.berkeley.edu/~jrs/papers/robustr.pdf
    static double const relativeError = 5.6e-16;
    // Because all 3 unit vectors are normalized, the maximum absolute value of
    // any vector component, cross product component or dot product term in
    // the calculation is very close to 1. The permanent of |M| must therefore
    // be below 3 + c, where c is some small multiple of ε. This constant, a
    // little larger than 3 * 5ε, is an upper bound on the absolute error in
    // the determinant calculation.
    static double const maxAbsoluteError = 1.7e-15;
    // This constant accounts for floating point underflow (assuming hardware
    // without gradual underflow, just to be conservative) in the computation
    // of det(M). It is a little more than 14 * 2^-1022.
    static double const minAbsoluteError = 4.0e-307;

    double bycz = b.y() * c.z();
    double bzcy = b.z() * c.y();
    double bzcx = b.z() * c.x();
    double bxcz = b.x() * c.z();
    double bxcy = b.x() * c.y();
    double bycx = b.y() * c.x();
    double determinant = a.x() * (bycz - bzcy) +
                         a.y() * (bzcx - bxcz) +
                         a.z() * (bxcy - bycx);
    if (determinant > maxAbsoluteError) {
        return 1;
    } else if (determinant < -maxAbsoluteError) {
        return -1;
    }
    // Expend some more effort on what is hopefully a tighter error bound
    // before falling back on arbitrary precision arithmetic.
    double permanent = std::fabs(a.x()) * (std::fabs(bycz) + std::fabs(bzcy)) +
                       std::fabs(a.y()) * (std::fabs(bzcx) + std::fabs(bxcz)) +
                       std::fabs(a.z()) * (std::fabs(bxcy) + std::fabs(bycx));
    double maxError = relativeError * permanent + minAbsoluteError;
    if (determinant > maxError) {
        return 1;
    } else if (determinant < -maxError) {
        return -1;
    }
    // Avoid the slow path when any two inputs are identical or antipodal.
    if (a == b || b == c || a == c || a == -b || b == -c || a == -c) {
        return 0;
    }
    return orientationExact(a, b, c);
}

}} // namespace lsst::sg
