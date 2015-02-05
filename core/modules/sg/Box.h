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

#ifndef LSST_SG_BOX_H_
#define LSST_SG_BOX_H_

/// \file
/// \brief This file declares a class for representing
///        longitude/latitude angle boxes on the unit sphere.

#include <iosfwd>

#include "AngleInterval.h"
#include "LonLat.h"
#include "NormalizedAngleInterval.h"
#include "Region.h"
#include "UnitVector3d.h"


namespace lsst {
namespace sg {

/// `Box` represents a rectangle in spherical coordinate space that contains
/// its boundary. A box can be empty or full (equal to the entire unit sphere),
/// and may contain just a single point. Besides the usual rectangular
/// regions, a box can also represent polar caps or annuli (i.e. when the box
/// spans all longitudes).
///
/// For any instance b of this class, the following properties hold:
///
/// - b.isEmpty() == b.getLat().isEmpty()
/// - b.getLat().isEmpty() == b.getLon().isEmpty()
/// - Box::allLatitudes().contains(b.getLat())
/// - Box::allLongitudes().contains(b.getLon())
class Box : public Region {
public:
    // Factory functions
    static Box fromDegrees(double lon1, double lat1, double lon2, double lat2) {
        return Box(NormalizedAngleInterval::fromDegrees(lon1, lon2),
                   AngleInterval::fromDegrees(lat1, lat2));
    }

    static Box fromRadians(double lon1, double lat1, double lon2, double lat2) {
        return Box(NormalizedAngleInterval::fromRadians(lon1, lon2),
                   AngleInterval::fromRadians(lat1, lat2));
    }

    static Box empty() { return Box(); }

    static Box full() { return Box(allLongitudes(), allLatitudes()); }

    /// `halfWidthForCircle` computes the half-width of bounding boxes
    /// for circles with radius `r` and centers at the given latitude.
    /// If r is non-positive, the result is zero, and if |lat| + r >= PI/2,
    /// the result is PI.
    static NormalizedAngle halfWidthForCircle(Angle r, Angle lat);

    /// `allLongitudes` returns a normalized angle interval containing
    /// all valid longitude angles.
    static NormalizedAngleInterval allLongitudes() {
        return NormalizedAngleInterval::full();
    }

    /// `allLatitudes` returns an angle interval containing all valid
    /// latitude angles.
    static AngleInterval allLatitudes() {
        return AngleInterval(Angle(-0.5 * PI), Angle(0.5 * PI));
    }

    /// This constructor creates an empty box.
    Box() {}

    /// This constructor creates a box containing a single point.
    explicit Box(LonLat const & p) :
        _lon(p.getLon()),
        _lat(p.getLat())
    {
        _enforceInvariants();
    }

    /// This constructor creates a box spanning the longitude interval
    /// [p1.getLon(), p2.getLon()] and latitude interval
    /// [p1.getLat(), p2.getLat()].
    Box(LonLat const & p1, LonLat const & p2) :
        _lon(p1.getLon(), p2.getLon()),
        _lat(p1.getLat(), p2.getLat())
    {
        _enforceInvariants();
    }

    /// This constructor creates a box with center p, width
    /// (in longitude angle) w and height (in latitude angle) h.
    Box(LonLat const & p, Angle w, Angle h) :
        _lon(NormalizedAngleInterval(p.getLon()).dilatedBy(w)),
        _lat(AngleInterval(p.getLat()).dilatedBy(h))
    {
        _enforceInvariants();
    }

    /// This constructor creates a box spanning the given
    /// longitude and latitude intervals.
    Box(NormalizedAngleInterval const & lon, AngleInterval const & lat) :
        _lon(lon),
        _lat(lat)
    {
        _enforceInvariants();
    }

    /// Two boxes are equal if they contain the same points.
    bool operator==(Box const & b) const {
        return _lon == b._lon && _lat == b._lat;
    }

    bool operator!=(Box const & b) const { return !(*this == b); }

    /// A box is equal to a point p if it contains only p.
    bool operator==(LonLat const & p) const {
        return _lat == p.getLat() && _lon == p.getLon();
    }

    bool operator!=(LonLat const & p) const { return !(*this == p); }

    /// `getLon` returns the longitude interval of this box.
    NormalizedAngleInterval const & getLon() const { return _lon; }

    /// `getLat` returns the latitude interval of this box.
    AngleInterval const & getLat() const { return _lat; }

    /// `isEmpty` returns true if this box does not contain any points.
    bool isEmpty() const { return _lat.isEmpty(); }

    /// `isFull` returns true if this box contains all points on
    /// the unit sphere.
    bool isFull() const { return _lon.isFull() && _lat == allLatitudes(); }

    /// `getCenter` returns the center of this box. It is NaN for empty
    /// boxes and arbitrary for full boxes.
    LonLat getCenter() const {
        return LonLat(_lon.getCenter(), _lat.getCenter());
    }

    /// `getWidth` returns the width in longitude angle of this box. It is NaN
    /// for empty boxes.
    NormalizedAngle getWidth() const { return _lon.getSize(); }

    /// `getHeight` returns the height in latitude angle of this box. It is
    /// negative or NaN for empty boxes.
    Angle getHeight() const { return _lat.getSize(); }

    ///@{
    /// `contains` returns true if the intersection of this box and x
    /// is equal to x.
    bool contains(LonLat const & x) const {
        return _lat.contains(x.getLat()) && _lon.contains(x.getLon());
    }

    bool contains(Box const & x) const {
        return _lat.contains(x._lat) && _lon.contains(x._lon);
    }
    ///@}

    ///@{
    /// `isDisjointFrom` returns true if the intersection of this box
    /// and x is empty.
    bool isDisjointFrom(LonLat const & x) const { return !intersects(x); }

    bool isDisjointFrom(Box const & x) const { return !intersects(x); }
    ///@}

    ///@{
    /// `intersects` returns true if the intersection of this box and x
    /// is non-empty.
    bool intersects(LonLat const & x) const {
        return _lat.intersects(x.getLat()) && _lon.intersects(x.getLon());
    }

    bool intersects(Box const & x) const {
        return _lat.intersects(x._lat) && _lon.intersects(x._lon);
    }
    ///@}

    ///@{
    /// `isWithin` returns true if the intersection of this box and x
    /// is this box.
    bool isWithin(LonLat const & x) const {
        return _lat.isWithin(x.getLat()) && _lon.isWithin(x.getLon());
    }

    bool isWithin(Box const & x) const {
        return _lat.isWithin(x._lat) && _lon.isWithin(x._lon);
    }
    ///@}

    /// `clipTo` shrinks this box until it contains only x. If this box
    /// does not contain x, it is emptied.
    Box & clipTo(LonLat const & x) {
        _lon.clipTo(x.getLon());
        _lat.clipTo(x.getLat());
        return *this;
    }

    /// `x.clipTo(y)` sets x to the smallest box containing the intersection
    /// of x and y. The result is not always unique, and `x.clipTo(y)` is not
    /// guaranteed to equal `y.clipTo(x)`.
    Box & clipTo(Box const & x) {
        _lon.clipTo(x.getLon());
        _lat.clipTo(x.getLat());
        return *this;
    }

    /// `clippedTo` returns the intersection of this box and x.
    Box clippedTo(LonLat const & x) const { return Box(*this).clipTo(x); }

    /// `clippedTo` returns the smallest box containing the intersection
    /// of this box and x. The result is not always unique, and
    /// `x.clippedTo(y)` is not guaranteed to equal `y.clippedTo(x)`.
    Box clippedTo(Box const & x) const { return Box(*this).clipTo(x); }

    ///@{
    /// `expandTo` minimally expands this box to contain x. The result
    /// is not always unique, and `x.expandTo(y)` is not guaranteed to equal
    /// `y.expandTo(x)`.
    Box & expandTo(LonLat const & x) {
        _lon.expandTo(x.getLon());
        _lat.expandTo(x.getLat());
        return *this;
    }

    Box & expandTo(Box const & x) {
        _lon.expandTo(x.getLon());
        _lat.expandTo(x.getLat());
        return *this;
    }
    ///@}

    ///@{
    /// `expandedTo` returns the smallest box containing the union of
    /// this interval and x. The result is not always unique, and
    /// `x.expandedTo(y)` is not guaranteed to equal `y.expandedTo(x)`.
    Box expandedTo(LonLat const & x) const { return Box(*this).expandTo(x); }
    Box expandedTo(Box const & x) const { return Box(*this).expandTo(x); }
    ///@}

    /// `dilateBy` minimally expands this Box to include all points within
    /// angular separation r of its boundary.
    ///
    /// If this box is empty or full, or if r is non-positive, there is
    /// no effect.
    Box & dilateBy(Angle r);
    Box dilatedBy(Angle r) const { return Box(*this).dilateBy(r); }

    /// `dilateBy` morphologically dilates or erodes the longitude interval
    /// of this box by w, and the latitude interval of this box by h. If
    /// w is positive, the longitude interval is dilated by [-w,w]. If w is
    /// zero, the corresponding interval is not modified, and if it is
    /// negative, the longitude interval is eroded by [w,-w]. The action of
    /// h on the latitude interval is analogous.
    ///
    /// If this box is empty or full, there is no effect. Furthermore, a box
    /// containing the north or south pole is not considered to have a
    /// latitude boundary there, so that eroding it will not necessarily
    /// remove the pole.
    ///
    /// If the desired outcome is the bounding box of points within some
    /// angular separation r of this box, then `dilateBy(r)` must be called,
    /// not `dilateBy(r, r)`.
    Box & dilateBy(Angle w, Angle h);
    Box dilatedBy(Angle w, Angle h) const { return Box(*this).dilateBy(w, h); }
    Box & erodeBy(Angle w, Angle h) { return dilateBy(-w, -h); }
    Box erodedBy(Angle w, Angle h) const { return dilatedBy(-w, -h);  }

    int relate(LonLat const & p) const { return relate(Box(p)); }

    /// `getArea` returns the area of this box in steradians.
    double getArea() const;

    // Region interface
    virtual Box * clone() const { return new Box(*this); }

    virtual Box getBoundingBox() const { return *this; }

    virtual Circle getBoundingCircle() const;

    virtual bool contains(UnitVector3d const & v) const {
        return contains(LonLat(v));
    }

    virtual int relate(Region const & r) const {
        // Dispatch on the type of r.
        return invertSpatialRelations(r.relate(*this));
    }

    virtual int relate(Box const & b) const {
        int lonrel = _lon.relate(b._lon);
        int latrel = _lat.relate(b._lat);
        // If the box longitude or latitude intervals are disjoint, then the
        // boxes are disjoint. The other spatial relationships must hold for
        // both the longitude and latitude intervals in order to hold for the
        // boxes.
        return ((lonrel & latrel) & (CONTAINS | INTERSECTS | WITHIN)) |
               ((lonrel | latrel) & DISJOINT);
    }

    virtual int relate(Circle const &) const;
    virtual int relate(ConvexPolygon const &) const;
    virtual int relate(Ellipse const &) const;

private:
    void _enforceInvariants() {
        // Make sure that _lat ⊆ [-π/2, π/2].
        _lat.clipTo(allLatitudes());
        // Make sure that both longitude and latitude intervals are
        // empty, or neither is. This simplifies the implementation
        // of the spatial relation tests.
        if (_lat.isEmpty()) {
            _lon = NormalizedAngleInterval();
        } else if (_lon.isEmpty()) {
            _lat = AngleInterval();
        }
    }

    NormalizedAngleInterval _lon;
    AngleInterval _lat;
};

std::ostream & operator<<(std::ostream &, Box const &);

}} // namespace lsst::sg

#endif // LSST_SG_BOX_H_
