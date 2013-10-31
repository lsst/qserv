/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

/// \file
/// \brief Machinery for spherical geometry and
///        Hierarchical Triangular Mesh indexing.

#ifndef LSST_QSERV_ADMIN_DUPR_GEOMETRY_H
#define LSST_QSERV_ADMIN_DUPR_GEOMETRY_H

#include <stdint.h>
#include <cmath>
#include <algorithm>
#include <utility>
#include <vector>

#include "Constants.h"
#include "Vector.h"


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

/// Clamp `lon` to be at most 360 degrees. Any input satisfying
///
///     lon >= 360.0 - EPSILON_DEG
///
/// is mapped to 360.0. This is useful when multiplying a (sub-)chunk width
/// by an integer to obtain (sub-)chunk bounds, as this multiplication is not
/// guaranteed to give a maximum longitude angle of exactly 360.0 degrees for
/// the last (sub-)chunk in a (sub-)stripe.
inline double clampLon(double lon) {
    if (lon > 360.0 - EPSILON_DEG) {
        return 360.0;
    }
    return lon;
}

/// Clamp `lat` to lie in the `[-90,90]` degree range.
inline double clampLat(double lat) {
    if (lat < -90.0) {
        return -90.0;
    } else if (lat > 90.0) {
        return 90.0;
    }
    return lat;
}

/// Return the minimum delta between two longitude angles,
/// both expected to be in degrees.
inline double minDeltaLon(double lon1, double lon2) {
    double delta = std::fabs(lon1 - lon2);
    return std::min(delta, 360.0 - delta);
}

/// Range reduce `lon` to lie in the `[0, 360)` degree range.
double reduceLon(double lon);

/// Compute the extent in longitude angle `[-α,α]` of the circle
/// with radius `r` and center `(0, centerLat)` on the unit sphere.
/// Both `r` and `centerLat` are assumed to be in units of degrees;
/// `centerLat` is clamped to lie in `[-90, 90]` and `r` must
/// lie in `[0, 90]`.
double maxAlpha(double r, double centerLat);

/// Compute the HTM ID of `v`.
uint32_t htmId(Vector3d const &v, int level);

/// Return the HTM subdivision level of `id` or -1 if `id` is invalid.
int htmLevel(uint32_t id);

/// Return the unit 3-vector corresponding to the given spherical
/// coordinates (in degrees).
Vector3d const cartesian(std::pair<double, double> const &lonLat);

inline Vector3d const cartesian(double lon, double lat) {
    return cartesian(std::pair<double, double>(lon, lat));
}

/// Return the longitude and latitude angles (in degrees) corresponding
/// to the given 3-vector.
std::pair<double, double> const spherical(Vector3d const &v);

inline std::pair<double, double> const spherical(double x, double y, double z) {
    return spherical(Vector3d(x,y,z));
}

/// Return the angular separation between `v0` and `v1` in radians.
double angSep(Vector3d const & v0, Vector3d const & v1);


class SphericalBox;

/// A triangle on the surface of the unit sphere with great-circle edges.
///
/// The main purpose of this class is to allow conversion between cartesian
/// 3-vectors and spherical barycentric coordinates.
///
/// The spherical barycentric coordinates b1, b2 and b3 of a 3-vector V,
/// given linearly independent triangle vertices V1, V2 and V3,
/// are defined as the solution to:
///
///     b1*V1 + b2*V2 + b3*V3 = V
///
/// If we let the column vector B = transpose([b1 b2 b3]) and M be the
/// 3x3 matrix with column vectors V1, V2 and V3, we can write the above
/// more simply as:
///
///     M * B = V
///
/// or
///
///     B = M⁻¹ * V
///
/// What are such coordinates used for?
///
/// Well, at a very high level, the Qserv data duplicator works by building a
/// map of non-empty HTM triangles. It converts the coordinates of each point
/// to spherical barycentric form. Then, to populate an empty triangle u, the
/// duplicator chooses a non-empty triangle v and copies all its points.
/// For a point V in v, the position of the copy is set to
///
///     Mᵤ * (Mᵥ⁻¹ * V) = (Mᵤ * Mᵥ⁻¹) * V
///
/// In other words, V is transformed by the matrix that maps the vertices of
/// v to the vertices of u. Since the area and proportions of different HTM
/// triangles don't vary all that much, one can think of (Mᵤ * Mᵥ⁻¹) as
/// something fairly close to a rotation. The fact that the transform isn't
/// quite length preserving doesn't matter; after all, cartesian coordinates
/// V and k*V (k > 0) map to the same spherical coordinates. Unlike an
/// approach that shifts around copies of an input data set in spherical
/// coordinate space, there are no serious distortion issues to worry about
/// near the poles.
///
/// Note that if the subdivision level of the target triangles is different
/// from that of the source triangles, the transform above can be used to
/// derive a catalog of greater or smaller density from an input catalog,
/// with relative angular structure roughly preserved.
///
/// See `QservDuplicate.cc` (the duplicator source code) to see this in action.
class SphericalTriangle {
public:
    /// Construct the HTM triangle with the given HTM ID.
    explicit SphericalTriangle(uint32_t htmId);

    /// Construct the triangle with the given vertices.
    SphericalTriangle(Vector3d const & v0, Vector3d const & v1, Vector3d const & v2);

    /// Get the i-th vertex (i = 0,1,2). No bounds checking is performed.
    Vector3d const & vertex(int i) const { return _m.col(i); }

    /// Get the matrix that converts from cartesian to
    /// spherical barycentric coordinates.
    Matrix3d const & getBarycentricTransform() const {
        return _mi;
    }
    /// Get the matrix that converts from spherical barycentric
    /// to cartesian coordinates.
    Matrix3d const & getCartesianTransform() const {
        return _m;
    }

    /// Compute the area of the triangle in steradians.
    double area() const;

    /// Compute the area of the surface obtained by intersecting this triangle
    /// with a spherical box. The routine is not fully general - for simplicity
    /// of implementation, spherical boxes with RA extent strictly between 180
    /// and 360 degrees are not supported.
    double intersectionArea(SphericalBox const & box) const;

private:
    /// [V0 V1 V2], where column vectors V0, V1, V2 are the triangle
    /// vertices (unit vectors).
    Matrix3d _m;
    /// Inverse of _m, corresponding to
    /// transpose([V1 x V2, V2 x V0, V0 x V1])/det(_m).
    Matrix3d _mi;
};


/// A spherical coordinate space bounding box.
///
/// This is similar to a bounding box in cartesian space in that it is
/// specified by a pair of points; however, a spherical box may correspond
/// to the entire unit-sphere, a spherical cap, a lune or the traditional
/// rectangle. Additionally, spherical boxes can span the 0/360 degree
/// longitude angle discontinuity.
class SphericalBox {
public:
    SphericalBox() : _lonMin(0.0), _lonMax(360.0), _latMin(-90.0), _latMax(90.0) { }

    SphericalBox(double lonMin, double lonMax, double latMin, double latMax);

    /// Create a conservative bounding box for a spherical triangle.
    SphericalBox(Vector3d const & v0, Vector3d const & v1, Vector3d const & v2);

    /// Expand the box by the given radius.
    void expand(double radius);

    bool isEmpty() const {
        return _latMax < _latMin;
    }
    bool isFull() const {
        return _latMin == -90.0 && _latMax ==  90.0 &&
               _lonMin ==   0.0 && _lonMax == 360.0;
    }

    /// Does the box wrap around the 0/360 degree longitude angle discontinuity?
    bool wraps() const {
        return _lonMax < _lonMin;
    }

    double getLonMin() const { return _lonMin; }
    double getLonMax() const { return _lonMax; }
    double getLatMin() const { return _latMin; }
    double getLatMax() const { return _latMax; }

    /// Compute the area of this box in steradians.
    double area() const;

    /// Return the extent in right ascension of this box.
    double getLonExtent() const {
        if (wraps()) {
            return 360.0 - _lonMin + _lonMax;
        }
        return _lonMax - _lonMin;
    }

    /// Does this box contain the given spherical coordinates?
    bool contains(double lon, double lat) const {
        if (lat < _latMin || lat > _latMax) {
            return false;
        }
        if (wraps()) {
           return lon >= _lonMin || lon <= _lonMax;
        }
        return lon >= _lonMin && lon <= _lonMax;
    }

    bool contains(std::pair<double, double> const & position) const {
        return contains(position.first, position.second);
    }

    /// Does this box intersect the given box?
    bool intersects(SphericalBox const & box) const {
        if (isEmpty() || box.isEmpty()) {
            return false;
        } else if (box._latMin > _latMax || box._latMax < _latMin) {
            return false;
        } else if (wraps()) {
            if (box.wraps()) {
                return true;
            }
            return box._lonMin <= _lonMax || box._lonMax >= _lonMin;
        } else if (box.wraps()) {
            return _lonMin <= box._lonMax || _lonMax >= box._lonMin;
        }
        return  _lonMin <= box._lonMax && _lonMax >= box._lonMin;
    }

    /// Compute a conservative approximation to the list of HTM triangles
    /// potentially overlapping this box and store them in `ids`.
    void htmIds(std::vector<uint32_t> & ids, int level) const;

private:
    void _findIds(std::vector<uint32_t> & ids,
                  uint32_t id,
                  int level,
                  Matrix3d const & m) const;

    double _lonMin;
    double _lonMax;
    double _latMin;
    double _latMax;
};

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_GEOMETRY_H
