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
/// \brief This file contains the Chunker class implementation.

#include "Chunker.h"

namespace lsst {
namespace sg {

namespace {

int32_t computeNumSegments(AngleInterval const & latitudes, Angle width) {
    // TODO(smm): Document this.
    if (width.asRadians() > PI) {
        return 1;
    }
    Angle maxAbsLat = std::max(abs(latitudes.getA()), abs(latitudes.getB()));
    if (maxAbsLat.asRadians() > 0.5 * PI - 4.85e-6) {
        return 1;
    }
    double cosWidth = cos(width);
    double sinLat = sin(maxAbsLat);
    double cosLat = cos(maxAbsLat);
    double x = cosWidth - sinLat * sinLat;
    double u = cosLat * cosLat;
    double y = std::sqrt(std::fabs(u * u - x * x));
    return static_cast<int32_t>(
        std::floor(2.0 * PI / std::fabs(std::atan2(y, x))));
}

} // unnamed namespace


Angle const Chunker::EPSILON(5.0e-12); // ~ 1 micro-arcsecond

Chunker::Chunker(int32_t numStripes,
                 int32_t numSubStripesPerStripe) :
    _numStripes(numStripes),
    _numSubStripesPerStripe(numSubStripesPerStripe),
    _numSubStripes(numStripes * numSubStripesPerStripe),
    _maxSubChunksPerSubStripeChunk(0),
    _subStripeHeight(Angle(PI) / _numSubStripes)
{
    if (numStripes < 1 || numSubStripesPerStripe < 1) {
        throw std::runtime_error("The number of stripes and sub-stripes "
                                 "per stripe must be positive");
    }
    if (numStripes * numSubStripesPerStripe > 180*3600) {
        throw std::runtime_error("Sub-stripes are too small");
    }
    Angle const stripeHeight = Angle(PI) / _numStripes;
    _stripes.reserve(_numStripes);
    _subStripes.reserve(_numSubStripes);
    for (int32_t s = 0; s < _numStripes; ++s) {
        // Compute stripe latitude bounds.
        AngleInterval sLat(s * stripeHeight - Angle(0.5 * PI),
                           (s + 1) * stripeHeight - Angle(0.5 * PI));
        Stripe stripe;
        int32_t const nc = computeNumSegments(sLat, stripeHeight);
        stripe.chunkWidth = Angle(2.0 * PI) / nc;
        stripe.numChunksPerStripe = nc;
        int32_t ss = s * _numSubStripesPerStripe;
        int32_t const ssEnd = ss + _numSubStripesPerStripe;
        for (; ss < ssEnd; ++ss) {
            // Compute sub-stripe latitude bounds.
            AngleInterval ssLat(ss * _subStripeHeight - Angle(0.5 * PI),
                                (ss + 1) * _subStripeHeight - Angle(0.5 * PI));
            SubStripe subStripe;
            int32_t const nsc = computeNumSegments(ssLat, _subStripeHeight) / nc;
            stripe.numSubChunksPerChunk += nsc;
            subStripe.numSubChunksPerChunk = nsc;
            if (nsc > _maxSubChunksPerSubStripeChunk) {
                _maxSubChunksPerSubStripeChunk = nsc;
            }
            subStripe.subChunkWidth = Angle(2.0 * PI) / (nsc * nc);
            _subStripes.push_back(subStripe);
        }
        _stripes.push_back(stripe);
    }
}

std::vector<int32_t> Chunker::getChunksIntersecting(Region const & r) const {
    std::vector<int32_t> chunkIds;
    // Find the stripes that intersect the bounding box of r.
    Box b = r.getBoundingBox().dilatedBy(EPSILON);
    double ya = std::floor((b.getLat().getA() + Angle(0.5 * PI)) / _subStripeHeight);
    double yb = std::floor((b.getLat().getB() + Angle(0.5 * PI)) / _subStripeHeight);
    int32_t minSS = std::min(static_cast<int32_t>(ya), _numSubStripes - 1);
    int32_t maxSS = std::min(static_cast<int32_t>(yb), _numSubStripes - 1);
    int32_t minS = minSS / _numSubStripesPerStripe;
    int32_t maxS = maxSS / _numSubStripesPerStripe;
    for (int32_t s = minS; s <= maxS; ++s) {
        // Find the chunks of s that intersect the bounding box of r.
        Angle chunkWidth = _stripes[s].chunkWidth;
        int32_t nc = _stripes[s].numChunksPerStripe;
        double xa = std::floor(b.getLon().getA() / chunkWidth);
        double xb = std::floor(b.getLon().getB() / chunkWidth);
        int32_t ca = std::min(static_cast<int32_t>(xa), nc - 1);
        int32_t cb = std::min(static_cast<int32_t>(xb), nc - 1);
        if (ca == cb && b.getLon().wraps()) {
            ca = 0;
            cb = nc - 1;
        }
        // Examine each chunk overlapping the bounding box of r.
        if (ca <= cb) {
            for (int32_t c = ca; c <= cb; ++c) {
                if (r.relate(_getChunkBoundingBox(s, c)) & INTERSECTS) {
                    chunkIds.push_back(_getChunkId(s, c));
                }
            }
        } else {
            for (int32_t c = 0; c <= cb; ++c) {
                if (r.relate(_getChunkBoundingBox(s, c)) & INTERSECTS) {
                    chunkIds.push_back(_getChunkId(s, c));
                }
            }
            for (int32_t c = ca; c < nc; ++c) {
                if (r.relate(_getChunkBoundingBox(s, c)) & INTERSECTS) {
                    chunkIds.push_back(_getChunkId(s, c));
                }
            }
        }
    }
    return chunkIds;
}

std::vector<SubChunks> Chunker::getSubChunksIntersecting(
    Region const & r) const
{
    std::vector<SubChunks> chunks;
    // Find the stripes that intersect the bounding box of r.
    Box b = r.getBoundingBox().dilatedBy(EPSILON);
    double ya = std::floor((b.getLat().getA() + Angle(0.5 * PI)) / _subStripeHeight);
    double yb = std::floor((b.getLat().getB() + Angle(0.5 * PI)) / _subStripeHeight);
    int32_t minSS = std::min(static_cast<int32_t>(ya), _numSubStripes - 1);
    int32_t maxSS = std::min(static_cast<int32_t>(yb), _numSubStripes - 1);
    int32_t minS = minSS / _numSubStripesPerStripe;
    int32_t maxS = maxSS / _numSubStripesPerStripe;
    for (int32_t s = minS; s <= maxS; ++s) {
        // Find the chunks of s that intersect the bounding box of r.
        Angle chunkWidth = _stripes[s].chunkWidth;
        int32_t nc = _stripes[s].numChunksPerStripe;
        double xa = std::floor(b.getLon().getA() / chunkWidth);
        double xb = std::floor(b.getLon().getB() / chunkWidth);
        int32_t ca = std::min(static_cast<int32_t>(xa), nc - 1);
        int32_t cb = std::min(static_cast<int32_t>(xb), nc - 1);
        if (ca == cb && b.getLon().wraps()) {
            ca = 0;
            cb = nc - 1;
        }
        // Examine sub-chunks for each chunk overlapping the bounding box of r.
        if (ca <= cb) {
            for (int32_t c = ca; c <= cb; ++c) {
                _getSubChunks(chunks, r, b.getLon(), s, c, minSS, maxSS);
            }
        } else {
            for (int32_t c = 0; c <= cb; ++c) {
                _getSubChunks(chunks, r, b.getLon(), s, c, minSS, maxSS);
            }
            for (int32_t c = ca; c < nc; ++c) {
                _getSubChunks(chunks, r, b.getLon(), s, c, minSS, maxSS);
            }
        }
    }
    return chunks;
}

void Chunker::_getSubChunks(std::vector<SubChunks> & chunks,
                            Region const & r,
                            NormalizedAngleInterval const & lon,
                            int32_t stripe,
                            int32_t chunk,
                            int32_t minSS,
                            int32_t maxSS) const
{
    SubChunks subChunks;
    subChunks.chunkId = _getChunkId(stripe, chunk);
    if (r.relate(_getChunkBoundingBox(stripe, chunk)) & CONTAINS) {
        // r contains the entire chunk, so there is no need to test sub-chunks
        // for intersection with r.
        subChunks.subChunkIds = getAllSubChunks(subChunks.chunkId);
    } else {
        // Find the sub-stripes to iterator over.
        minSS = std::max(minSS, stripe * _numSubStripesPerStripe);
        maxSS = std::min(maxSS, (stripe + 1) * _numSubStripesPerStripe - 1);
        int32_t const nc = _stripes[stripe].numChunksPerStripe;
        for (int32_t ss = minSS; ss <= maxSS; ++ss) {
            // Find the sub-chunks of ss to iterate over.
            Angle subChunkWidth = _subStripes[ss].subChunkWidth;
            int32_t const nsc = _subStripes[ss].numSubChunksPerChunk;
            double xa = std::floor(lon.getA() / subChunkWidth);
            double xb = std::floor(lon.getB() / subChunkWidth);
            int32_t sca = std::min(static_cast<int32_t>(xa), nc * nsc - 1);
            int32_t scb = std::min(static_cast<int32_t>(xb), nc * nsc - 1);
            if (sca == scb && lon.wraps()) {
                sca = 0;
                scb = nc * nsc - 1;
            }
            int32_t minSC = chunk * nsc;
            int32_t maxSC = (chunk + 1) * nsc - 1;
            // Test each sub-chunk against r, and record those that intersect.
            if (sca <= scb) {
                minSC = std::max(sca, minSC);
                maxSC = std::min(scb, maxSC);
                for (int32_t sc = minSC; sc <= maxSC; ++sc) {
                    if (r.relate(_getSubChunkBoundingBox(ss, sc)) & INTERSECTS) {
                        subChunks.subChunkIds.push_back(
                            _getSubChunkId(stripe, ss, chunk, sc));
                    }
                }
            } else {
                sca = std::max(sca, minSC);
                scb = std::min(scb, maxSC);
                for (int32_t sc = sca; sc <= maxSC; ++sc) {
                    if (r.relate(_getSubChunkBoundingBox(ss, sc)) & INTERSECTS) {
                        subChunks.subChunkIds.push_back(
                            _getSubChunkId(stripe, ss, chunk, sc));
                    }
                }
                for (int32_t sc = minSC; sc <= scb; ++sc) {
                    if (r.relate(_getSubChunkBoundingBox(ss, sc)) & INTERSECTS) {
                        subChunks.subChunkIds.push_back(
                            _getSubChunkId(stripe, ss, chunk, sc));
                    }
                }
            }
        }
    }
    // If any sub-chunks of this chunk intersect r,
    // append them to the result vector.
    if (!subChunks.subChunkIds.empty()) {
        chunks.push_back(SubChunks());
        chunks.back().swap(subChunks);
    }
}

std::vector<int32_t> Chunker::getAllChunks() const {
    std::vector<int32_t> chunkIds;
    for (int32_t s = 0; s < _numStripes; ++s) {
        int32_t nc = _stripes[s].numChunksPerStripe;
        for (int32_t c = 0; c < nc; ++c) {
            chunkIds.push_back(_getChunkId(s, c));
        }
    }
    return chunkIds;
}

std::vector<int32_t> Chunker::getAllSubChunks(int32_t chunkId) const {
    std::vector<int32_t> subChunkIds;
    int32_t s = _getStripe(chunkId);
    subChunkIds.reserve(_stripes.at(s).numSubChunksPerChunk);
    int32_t ss = s * _numSubStripesPerStripe;
    int32_t const ssEnd = ss + _numSubStripesPerStripe;
    for (; ss < ssEnd; ++ss) {
        int32_t const scEnd = _subStripes[ss].numSubChunksPerChunk;
        int32_t const subChunkIdBase = _maxSubChunksPerSubStripeChunk * ss;
        for (int32_t sc = 0; sc < scEnd; ++sc) {
            subChunkIds.push_back(subChunkIdBase + sc);
        }
    }
    return subChunkIds;
}

Box Chunker::_getChunkBoundingBox(int32_t stripe, int32_t chunk) const {
    Angle chunkWidth = _stripes[stripe].chunkWidth;
    NormalizedAngleInterval lon(chunkWidth * chunk,
                                chunkWidth * (chunk + 1));
    int32_t ss = stripe * _numSubStripesPerStripe;
    int32_t ssEnd = ss + _numSubStripesPerStripe;
    AngleInterval lat(ss * _subStripeHeight - Angle(0.5 * PI),
                      ssEnd * _subStripeHeight - Angle(0.5 * PI));
    return Box(lon, lat).dilatedBy(EPSILON);
}

Box Chunker::_getSubChunkBoundingBox(int32_t subStripe, int32_t subChunk) const {
    Angle subChunkWidth = _subStripes[subStripe].subChunkWidth;
    NormalizedAngleInterval lon(subChunkWidth * subChunk,
                                subChunkWidth * (subChunk + 1));
    AngleInterval lat(subStripe * _subStripeHeight - Angle(0.5 * PI),
                      (subStripe + 1) * _subStripeHeight - Angle(0.5 * PI));
    return Box(lon, lat).dilatedBy(EPSILON);
}

}} // namespace lsst::sg
