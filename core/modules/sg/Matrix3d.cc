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
/// \brief This file contains the Matrix3d class implementation.

#include "Matrix3d.h"

#include <cstdio>
#include <ostream>


namespace lsst {
namespace sg {

void Matrix3d::print(std::ostream & os, int indent) const {
    char e[3][3][32]; // element strings
    int w[3][3]; // element widths
    int cw[3] = {0, 0, 0}; // column widths
    Matrix3d const & m = *this;
    // Convert each matrix element to a string. Compute column widths
    // such that the elements of each matrix column are aligned.
    for (int r = 0; r < 3; ++r) {
        for (int c = 0; c < 3; ++c) {
            w[r][c] = static_cast<int>(
                std::snprintf(e[r][c], 32, "%.17g", m(r,c)));
            cw[c] = std::max(cw[c], w[r][c]);
        }
    }
    for (int r = 0; r < 3; ++r) {
        // Write out line indentation.
        for (int i = 0; i < indent; ++i) {
            os.put(' ');
        }
        // Output the type name or the equivalent number of spaces.
        if (r == 0) {
            os.write("Matrix3d(", 9);
        } else {
            os.write("         ", 9);
        }
        // Output right-aligned matrix elements for row r.
        for (int c = 0; c < 3; ++c) {
            for (int i = cw[c] - w[r][c]; i > 0; --i) {
                os.put(' ');
            }
            os.write(e[r][c], w[r][c]);
            if (c < 2) {
                os.write(", ", 2);
            } else if (r < 2) {
                os.write(",\n", 2);
            } else {
                os.put(')');
            }
        }
    }
}

std::ostream & operator<<(std::ostream & os, Matrix3d const & m) {
    m.print(os, 0);
    return os;
}

}} // namespace lsst::sg
