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

#ifndef LSST_SG_MATRIX3D_H_
#define LSST_SG_MATRIX3D_H_

/// \file
/// \brief This file contains a class representing 3x3 real matrices.

#include <iosfwd>

#include "Vector3d.h"


namespace lsst {
namespace sg {

/// A 3x3 matrix with real entries stored in double precision.
class Matrix3d {
public:
    /// This constructor creates a zero matrix.
    Matrix3d() {}

    /// This constructor creates a matrix from its components,
    /// where `mij` specifies the component for row `i` and column `j`.
    Matrix3d(double m00, double m01, double m02,
             double m10, double m11, double m12,
             double m20, double m21, double m22)
    {
        Matrix3d & m = *this;
        m(0, 0) = m00; m(0, 1) = m01; m(0, 2) = m02;
        m(1, 0) = m10; m(1, 1) = m11; m(1, 2) = m12;
        m(2, 0) = m20; m(2, 1) = m21; m(2, 2) = m22;
    }

    /// This constructor creates a diagonal matrix with diagonal components
    /// set to the components of `v`.
    explicit Matrix3d(Vector3d const & v) {
        Matrix3d & m = *this;
        m(0, 0) = v(0);
        m(1, 1) = v(1);
        m(2, 2) = v(2);
    }

    /// This constructor returns the identity matrix scaled by `s`.
    explicit Matrix3d(double s) {
        Matrix3d & m = *this;
        m(0, 0) = s;
        m(1, 1) = s;
        m(2, 2) = s;
    }

    bool operator==(Matrix3d const & m) const {
        return _c[0] == m._c[0] &&
               _c[1] == m._c[1] &&
               _c[2] == m._c[2];
    }

    bool operator!=(Matrix3d const & m) const {
        return _c[0] != m._c[0] ||
               _c[1] != m._c[1] ||
               _c[2] != m._c[2];
    }

    ///@{
    /// `getColumn` returns the `c`-th matrix column. Bounds are not checked.
    Vector3d & getColumn(int c) { return _c[c]; }
    Vector3d const & getColumn(int c) const { return _c[c]; }
    ///@}

    ///@{
    /// The function call operator returns the scalar at row `r` and column `c`.
    /// Bounds are not checked.
    double & operator()(int r, int c) { return getColumn(c)(r); }
    double operator()(int r, int c) const { return getColumn(c)(r); }
    ///@}

    /// `inner` returns the Frobenius inner product of this matrix with `m`.
    double inner(Matrix3d const & m) const {
        Matrix3d p = cwiseProduct(m);
        Vector3d sum = p.getColumn(0) + p.getColumn(1) + p.getColumn(2);
        return sum(0) + sum(1) + sum(2);
    }

    /// `getSquaredNorm` returns the Frobenius inner product of this matrix
    /// with itself.
    double getSquaredNorm() const { return inner(*this); }

    /// `getNorm` returns the L2 (Frobenius) norm of this matrix.
    double getNorm() const { return std::sqrt(getSquaredNorm()); }

    /// The multiplication operator returns the product of this matrix
    /// with vector `v`.
    Vector3d operator*(Vector3d const & v) const {
        return Vector3d(getColumn(0) * v(0) +
                        getColumn(1) * v(1) +
                        getColumn(2) * v(2));
    }

    /// The multiplication operator returns the product of this matrix
    /// with matrix `m`.
    Matrix3d operator*(Matrix3d const & m) const {
        Matrix3d r;
        for (int i = 0; i < 3; ++i) {
            r.getColumn(i) = this->operator*(m.getColumn(i));
        }
        return r;
    }

    /// The addition operator returns the sum of this matrix and `m`.
    Matrix3d operator+(Matrix3d const & m) const {
        Matrix3d r;
        for (int i = 0; i < 3; ++i) {
            r.getColumn(i) = getColumn(i) + m.getColumn(i);
        }
        return r;
    }

    /// The subtraction operator returns the difference between this matrix and `m`.
    Matrix3d operator-(Matrix3d const & m) const {
        Matrix3d r;
        for (int i = 0; i < 3; ++i) {
            r.getColumn(i) = getColumn(i) - m.getColumn(i);
        }
        return r;
    }

    /// `cwiseProduct` returns the component-wise product of this matrix and `m`.
    Matrix3d cwiseProduct(Matrix3d const & m) const {
        Matrix3d r;
        for (int i = 0; i < 3; ++i) {
            r.getColumn(i) = getColumn(i).cwiseProduct(m.getColumn(i));
        }
        return r;
    }

    /// `transpose` returns the transpose of this matrix.
    Matrix3d transpose() const {
        Matrix3d t;
        Matrix3d const &m = *this;
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 3; ++j) {
                t(i, j) = m(j, i);
            }
        }
        return t;
    }

    /// `inverse` returns the inverse of this matrix.
    Matrix3d inverse() const {
        Matrix3d inv;
        Matrix3d const & m = *this;
        // Find the first column of Adj(m), the adjugate matrix of m.
        Vector3d a0(m(1, 1) * m(2, 2) - m(2, 1) * m(1, 2),
                    m(1, 2) * m(2, 0) - m(2, 2) * m(1, 0),
                    m(1, 0) * m(2, 1) - m(2, 0) * m(1, 1));
        // Find 1.0/det(m), where the determinant of m is the dot product of
        // the first row of m with the first column of Adj(m).
        double rdet = 1.0 / (a0(0) * m(0,0) + a0(1) * m(0,1) + a0(2) * m(0,2));
        // The inverse of m is Adj(m)/det(m); compute it column by column.
        inv.getColumn(0) = a0 * rdet;
        inv(0,1) = (m(0, 2) * m(2, 1) - m(2, 2) * m(0, 1)) * rdet;
        inv(1,1) = (m(0, 0) * m(2, 2) - m(2, 0) * m(0, 2)) * rdet;
        inv(2,1) = (m(0, 1) * m(2, 0) - m(2, 1) * m(0, 0)) * rdet;
        inv(0,2) = (m(0, 1) * m(1, 2) - m(1, 1) * m(0, 2)) * rdet;
        inv(1,2) = (m(0, 2) * m(1, 0) - m(1, 2) * m(0, 0)) * rdet;
        inv(2,2) = (m(0, 0) * m(1, 1) - m(1, 0) * m(0, 1)) * rdet;
        return inv;
    }

    /// `print` writes this matrix to the given output stream. Each line of
    /// output is indented by the given number of spaces.
    void print(std::ostream & os, int indent) const;

private:
    Vector3d _c[3];
};

std::ostream & operator<<(std::ostream &, Matrix3d const &);

}} // namespace lsst::sg

#endif // LSST_SG_MATRIX3D_H_
