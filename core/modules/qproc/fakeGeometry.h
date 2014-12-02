// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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
#ifndef LSST_QSERV_QPROC_FAKEGEOMETRY_H
#define LSST_QSERV_QPROC_FAKEGEOMETRY_H
/**
  * @file
  *
  * @brief Fake geometry interface code for testing
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <vector>

// Other external headers
#include "boost/make_shared.hpp"

// Qserv headers
#include "css/StripingParams.h"
#include "qproc/QueryProcessingBug.h"

namespace lsst {
namespace qserv {
namespace qproc {

// Temporary
typedef double Coordinate;
class Region {
public:
    virtual ~Region() {}
};
typedef boost::shared_ptr<Region> RegionPtr;
typedef std::vector<RegionPtr> RegionPtrVector;
typedef std::pair<Coordinate, Coordinate> UnitVector3d;

class BoxRegion : public Region {
public:

    static BoxRegion fromDeg(Coordinate lon1, Coordinate lat1,
                             Coordinate lon2, Coordinate lat2) {
        return BoxRegion(); // Fake.
    }
private:
    BoxRegion() {}
};

class CircleRegion : public Region {
public:
    // FIXME: Is circle chord squared in radians? probably...
    CircleRegion(std::pair<Coordinate, Coordinate> const& u, Coordinate cl2) {
    }
private:
    CircleRegion() {}
};

class EllipseRegion : public Region {
public:
    EllipseRegion(UnitVector3d const& center,
                  double alphaRad,
                  double betaRad,
                  double orientRad) {}
private:
    EllipseRegion(std::vector<Coordinate> const& params) {}
};

class ConvexPolyRegion : public Region {
public:
    ConvexPolyRegion(std::vector<UnitVector3d> const& vertices) {}

private:
    ConvexPolyRegion() {}
};

struct ChunkTuple { // Geometry will have a struct like this.
    int chunkId;
    std::vector<int> subChunkIds;
    static ChunkTuple makeFake(int i) {
        ChunkTuple t;
        t.chunkId = i;
        for (int sc=0; sc < 3; ++i) {
            t.subChunkIds.push_back((sc*10) + i);
        }
        return t;
    }
};
typedef std::vector<ChunkTuple> ChunkRegion;

class PartitioningMap {
public:
    /// Placeholder
    explicit PartitioningMap(css::StripingParams const& sp)
        : _stripes(sp.stripes),
          _subStripes(sp.subStripes) {
    }

    boost::shared_ptr<ChunkRegion> intersect(Region const& r) {
        boost::shared_ptr<ChunkRegion> cr = boost::make_shared<ChunkRegion>();
        cr->push_back(ChunkTuple::makeFake(1));
        cr->push_back(ChunkTuple::makeFake(2));
        return cr;
    }
    ChunkRegion getIntersect(Region const& r) {
        ChunkRegion cr;
        cr.push_back(ChunkTuple::makeFake(1000));
        return cr;
    }

    ChunkRegion getIntersect(RegionPtrVector const& rv) {
        ChunkRegion cr;
        cr.push_back(ChunkTuple::makeFake(100));
        cr.push_back(ChunkTuple::makeFake(200));
        cr.push_back(ChunkTuple::makeFake(300));
        return cr;
    }

    int _stripes;
    int _subStripes;
};

}}} // namespace lsst::qserv::ccontrol

#endif // LSST_QSERV_CCONTROL_FAKEGEOMETRY_H

