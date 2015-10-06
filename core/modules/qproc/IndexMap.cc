// -*- LSST-C++ -*-
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
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

/**
  * @file
  *
  * @brief Implementation of IndexMap
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qproc/IndexMap.h"

// System headers
#include <algorithm>
#include <cassert>
#include <iterator>
#include <stdexcept>
#include <set>
#include <vector>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"
#include "sg/Chunker.h"

// Qserv headers
#include "global/Bug.h"
#include "global/intTypes.h"
#include "global/stringTypes.h"
#include "qproc/geomAdapter.h"
#include "qproc/QueryProcessingError.h"
#include "qproc/SecondaryIndex.h"
#include "query/Constraint.h"
#include "util/IterableFormatter.h"

using lsst::qserv::StringVector;
using lsst::sg::Region;
using lsst::sg::Box;
using lsst::sg::Circle;
using lsst::sg::Ellipse;
using lsst::sg::ConvexPolygon;
using lsst::sg::SubChunks;

typedef std::vector<SubChunks> SubChunksVector;

namespace {
LOG_LOGGER getLogger() {
    static LOG_LOGGER logger = LOG_GET("lsst.qserv.qproc.IndexMap");
    return logger;
}
}

namespace { // File-scope helpers
template <typename T>
std::vector<T> convertVec(StringVector const& v) {
    std::vector<T> out;
    out.reserve(v.size());
    std::transform(v.begin(), v.end(), std::back_inserter(out),
                   boost::lexical_cast<T, std::string>);
    return out;
}
template <typename T>
std::shared_ptr<Region> make(StringVector const& v) {
    return std::shared_ptr<Region>(new T(convertVec<double>(v)));
}
template <>
std::shared_ptr<Region> make<Box>(StringVector const& v) {
    return lsst::qserv::qproc::getBoxFromParams(convertVec<double>(v));
}
template <>
std::shared_ptr<Region> make<Circle>(StringVector const& v) {
    return lsst::qserv::qproc::getCircleFromParams(convertVec<double>(v));
}
template <>
std::shared_ptr<Region> make<Ellipse>(StringVector const& v) {
    return lsst::qserv::qproc::getEllipseFromParams(convertVec<double>(v));
}
template <>
std::shared_ptr<Region> make<ConvexPolygon>(StringVector const& v) {
    return lsst::qserv::qproc::getConvexPolyFromParams(convertVec<double>(v));
}

typedef std::shared_ptr<Region>(*MakeFunc)(StringVector const& v);

struct FuncMap {
    FuncMap() {
        fMap["box"] = make<Box>;
        fMap["circle"] = make<Circle>;
        fMap["ellipse"] = make<Ellipse>;
        fMap["poly"] = make<ConvexPolygon>;
        fMap["qserv_areaspec_box"] = make<Box>;
        fMap["qserv_areaspec_circle"] = make<Circle>;
        fMap["qserv_areaspec_ellipse"] = make<Ellipse>;
        fMap["qserv_areaspec_poly"] = make<ConvexPolygon>;
    }
    typedef std::map<std::string, MakeFunc> Map;
    Map fMap;
};
static FuncMap funcMap;

/*  Computes region covered by a given spherical geometry UDF call
 *
 *  @param c:   Constraint containing name and parameter of UDF call
 *  @return:    Pointer to Region covered by c, or nullptr if empty
 */
std::shared_ptr<Region> getRegion(lsst::qserv::query::Constraint const& c) {
    std::shared_ptr<Region> covered_region = nullptr;
    FuncMap::Map::const_iterator i = funcMap.fMap.find(c.name);
    if(i != funcMap.fMap.end()) {
        LOGF(getLogger(), LOG_LVL_TRACE, "Region for %1%: %2%" % c % i->first);
        covered_region = i->second(c.params);
    }
    return covered_region;
}

lsst::qserv::qproc::ChunkSpec convertSgSubChunks(SubChunks const& sc) {
    lsst::qserv::qproc::ChunkSpec cs;
    cs.chunkId = sc.chunkId;
    cs.subChunks.resize(sc.subChunkIds.size());
    std::copy(sc.subChunkIds.begin(), sc.subChunkIds.end(),
              cs.subChunks.begin());
    return cs;
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qproc {
typedef std::vector<std::shared_ptr<Region> > RegionPtrVector;

////////////////////////////////////////////////////////////////////////
// IndexMap::PartitioningMap definition and implementation
////////////////////////////////////////////////////////////////////////
class IndexMap::PartitioningMap {
public:
    class NoRegion : public std::invalid_argument {
    public:
        NoRegion() : std::invalid_argument("No region specified")
            {}
    };
    explicit PartitioningMap(css::StripingParams const& sp) {
        _chunker = std::make_shared<sg::Chunker>(sp.stripes,
                                                 sp.subStripes);

    }
    /// @return un-canonicalized vector<SubChunks> of concatenated region
    /// results. Regions are assumed to be joined by implicit "OR" and not "AND"
    /// Throws NoRegion if no region is passed.
    SubChunksVector getIntersect(RegionPtrVector const& rv) {
        SubChunksVector scv;
        bool hasRegion = false;
        for(RegionPtrVector::const_iterator i=rv.begin(), e=rv.end();
            i != e;
            ++i) {
            if(*i) {
                SubChunksVector area = getCoverage(**i);
                scv.insert(scv.end(), area.begin(), area.end());
                hasRegion = true;
            } else {
                // Ignore null-regions
                continue;
            }
        }
        if(!hasRegion) {
            throw NoRegion();
        }
        return scv;
    }

    inline SubChunksVector getCoverage(Region const& r) {
        return _chunker->getSubChunksIntersecting(r);
    }
    ChunkSpecVector getAllChunks() const {
        Int32Vector allChunks = _chunker->getAllChunks();
        ChunkSpecVector csv;
        csv.reserve(allChunks.size());
        for(IntVector::const_iterator i=allChunks.begin(), e=allChunks.end();
            i != e; ++i) {
            csv.push_back(ChunkSpec(*i, _chunker->getAllSubChunks(*i)));
        }
        return csv;
    }
private:
    std::shared_ptr<sg::Chunker> _chunker;
};

////////////////////////////////////////////////////////////////////////
// IndexMap implementation
////////////////////////////////////////////////////////////////////////
IndexMap::IndexMap(css::StripingParams const& sp,
                   std::shared_ptr<SecondaryIndex> si)
    : _pm(std::make_shared<PartitioningMap>(sp)),
      _si(si) {
}

// Compute the chunks list for the whole partitioning scheme
ChunkSpecVector IndexMap::getAllChunks() {
    return _pm->getAllChunks();
}

//  Compute chunks coverage of spatial and secondary index constraints
ChunkSpecVector IndexMap::getChunks(query::ConstraintVector const& cv) {

    // Secondary Index lookups
    if(!_si) {
        throw Bug("Invalid SecondaryIndex in IndexMap. Check IndexMap(...)");
    }
    ChunkSpecVector indexSpecs;
    bool hasIndex = true;
    bool hasRegion = true;
    try {
        indexSpecs = _si->lookup(cv);
        LOGF(getLogger(), LOG_LVL_TRACE, "Index specs: %1%" % util::printable(indexSpecs));
    } catch(SecondaryIndex::NoIndexConstraint& e) {
        hasIndex = false; // Ok if no index constraints
    }

    // Spatial area lookups
    RegionPtrVector rv;
    std::transform(cv.begin(), cv.end(), std::back_inserter(rv), getRegion);
    SubChunksVector scv;
    try {
        scv = _pm->getIntersect(rv);
    } catch(PartitioningMap::NoRegion& e) {
        hasRegion = false;
    } catch(std::invalid_argument& a) {
        throw QueryProcessingError(a.what());
    } catch(std::runtime_error& e) {
        throw QueryProcessingError(e.what());
    }
    ChunkSpecVector regionSpecs;
    std::transform(scv.begin(), scv.end(),
                   std::back_inserter(regionSpecs), convertSgSubChunks);

    // FIXME: Index and spatial lookup are supported in AND format only right now.
    if(hasIndex && hasRegion) {
        // Perform AND with index and spatial
        normalize(indexSpecs);
        normalize(regionSpecs);
        intersectSorted(indexSpecs, regionSpecs);
        return indexSpecs;
    } else if(hasIndex) {
        return indexSpecs;
    } else if(hasRegion) {
        return regionSpecs;
    } else {
        return getAllChunks();
    }
}

}}} // namespace lsst::qserv::qproc


