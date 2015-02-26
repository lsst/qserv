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

#ifndef LSST_SG_CHUNKER_H_
#define LSST_SG_CHUNKER_H_

/// \file
/// \brief This file declares a class for partitioning the sky into chunks
///        and sub-chunks.

#include <stdint.h>
#include <vector>

#include "Angle.h"
#include "Box.h"


namespace lsst {
namespace sg {

/// `SubChunks` represents a set of sub-chunks of a particular chunk.
///
/// TODO(smm): implement a more memory efficient representation than this.
struct SubChunks {
    int32_t chunkId;
    std::vector<int32_t> subChunkIds;

    SubChunks() : chunkId(-1) {}

    void swap(SubChunks & sc) {
        std::swap(chunkId, sc.chunkId);
        subChunkIds.swap(sc.subChunkIds);
    }
};


/// `Chunker` subdivides the unit sphere into longitude-latitude boxes.
///
/// The unit sphere is divided into latitude angle "stripes" of fixed
/// height H. For each stripe, a width W is computed such that any two points
/// in the stripe with longitude angles separated by at least W have angular
/// separation of at least H. The stripe is then broken into an integral number
/// of chunks of width at least W. The same procedure is used to obtain finer
/// subchunks - each stripe is broken into a configureable number of
/// equal-height "substripes", and each substripe is broken into equal-width
/// subchunks.
class Chunker {
public:
    Chunker(int32_t numStripes,
            int32_t numSubStripesPerStripe);

    /// `getNumStripes` returns the number of fixed-height latitude intervals
    /// in the sky subdivision.
    int32_t getNumStripes() const {
        return _numStripes;
    }

    /// `getNumSubStripesPerStripe` returns the number of fixed-height latitude
    /// sub-intervals in each stripe.
    int32_t getNumSubStripesPerStripe() const {
        return _numSubStripesPerStripe;
    }

    /// `getChunksIntersecting` returns all the chunks that potentially
    /// intersect the given region.
    std::vector<int32_t> getChunksIntersecting(Region const & r) const;

    /// `getSubChunksIntersecting` returns all the sub-chunks that potentially
    /// intersect the given region.
    std::vector<SubChunks> getSubChunksIntersecting(Region const & r) const;

    /// `getAllChunks` returns the complete set of chunk IDs for the unit
    /// sphere.
    std::vector<int32_t> getAllChunks() const;

    /// `getAllSubChunks` returns the complete set of sub-chunk IDs
    /// for the given chunk.
    std::vector<int32_t> getAllSubChunks(int32_t chunkId) const;

private:
    struct Stripe {
        Angle chunkWidth;
        int32_t numChunksPerStripe;
        int32_t numSubChunksPerChunk;

        Stripe() :
            chunkWidth(0),
            numChunksPerStripe(0),
            numSubChunksPerChunk(0)
        {}
    };

    struct SubStripe {
        Angle subChunkWidth;
        int32_t numSubChunksPerChunk;

        SubStripe() : subChunkWidth(), numSubChunksPerChunk(0) {}
    };

    static Angle const EPSILON;

    int32_t _getStripe(int32_t chunkId) const {
        return chunkId / (2 * _numStripes);
    }

    int32_t _getChunkId(int32_t stripe, int32_t chunk) const {
        return stripe * 2 * _numStripes + chunk;
    }

    int32_t _getSubChunkId(int32_t stripe, int32_t subStripe,
                           int32_t chunk, int32_t subChunk) const {
        int32_t y = subStripe - stripe * _numSubStripesPerStripe;
        int32_t x = subChunk -
                    chunk * _subStripes[subStripe].numSubChunksPerChunk;
        return y * _maxSubChunksPerSubStripeChunk + x;
    }

    void _getSubChunks(std::vector<SubChunks> & subChunks,
                       Region const & r,
                       NormalizedAngleInterval const & lon,
                       int32_t stripe,
                       int32_t chunk,
                       int32_t minSS,
                       int32_t maxSS) const;
    Box _getChunkBoundingBox(int32_t stripe, int32_t chunk) const;
    Box _getSubChunkBoundingBox(int32_t subStripe, int32_t subChunk) const;

    int32_t _numStripes;
    int32_t _numSubStripesPerStripe;
    int32_t _numSubStripes;
    int32_t _maxSubChunksPerSubStripeChunk;
    Angle _subStripeHeight;
    std::vector<Stripe> _stripes;
    std::vector<SubStripe> _subStripes;
};

}} // namespace lsst::sg

#endif // LSST_SG_CHUNKER_H_
