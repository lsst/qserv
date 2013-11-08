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

#include "ChunkIndex.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <stdexcept>
#include <utility>

#include "boost/scoped_array.hpp"

#include "Constants.h"
#include "FileUtils.h"

using std::floor;
using std::numeric_limits;
using std::ostream;
using std::pair;
using std::pow;
using std::runtime_error;
using std::setprecision;
using std::setw;
using std::sort;
using std::sqrt;
using std::string;
using std::vector;

namespace fs = boost::filesystem;


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

void ChunkIndex::Stats::clear() {
    n = 0;
    min = 0;
    max = 0;
    quartile[0] = 0; quartile[1] = 0; quartile[2] = 0;
    sigma = numeric_limits<double>::quiet_NaN();
    skewness = numeric_limits<double>::quiet_NaN();
    kurtosis = numeric_limits<double>::quiet_NaN();
}

namespace {
    uint64_t percentile(double p, vector<uint64_t> & v) {
        typedef vector<uint64_t>::size_type S;
        S i = std::min(static_cast<S>(floor(p*v.size() + 0.5)), v.size() - 1);
        return v[i];
    }
}

void ChunkIndex::Stats::computeFrom(vector<uint64_t> & counts) {
    typedef vector<uint64_t>::iterator Iter;
    if (counts.empty()) {
        clear();
        return;
    }
    n = counts.size();
    nrec = 0;
    max = 0;
    min = numeric_limits<uint64_t>::max();
    // Compute sum, min, and max of record counts.
    for (Iter i = counts.begin(), e = counts.end(); i != e; ++i) {
        uint64_t c = *i;
        nrec += c;
        if (c < min) { min = c; }
        if (c > max) { max = c; }
    }
    sort(counts.begin(), counts.end());
    n = counts.size();
    quartile[0] = percentile(0.25, counts);
    quartile[1] = percentile(0.5,  counts);
    quartile[2] = percentile(0.75, counts);
    mean = static_cast<double>(nrec) / static_cast<double>(n);
    // Compute moments of the record count distribution.
    double m2 = 0.0, m3 = 0.0, m4 = 0.0;
    for (Iter c = counts.begin(), e = counts.end(); c != e; ++c) {
        double d = static_cast<double>(*c) - mean;
        double d2 = d*d;
        m2 += d2;
        m3 += d2*d;
        m4 += d2*d2;
    }
    m2 /= n;
    m3 /= n;
    m4 /= n;
    sigma = sqrt(m2);
    skewness = m3/pow(m2, 1.5);
    kurtosis = m4/(m2*m2) - 3.0;
}

void ChunkIndex::Stats::write(ostream & os, string const & indent) const {
    os << indent << "\"nrec\":      " << nrec << ",\n"
       << indent << "\"n\":         " << n << ",\n"
       << indent << "\"min\":       " << min << ",\n"
       << indent << "\"max\":       " << max << ",\n"
       << indent << "\"quartile\": [" << quartile[0] << ", "
                                      << quartile[1] << ", "
                                      << quartile[2] << "],\n"
       << indent << "\"mean\":      " << setprecision(3) << mean << ",\n"
       << indent << "\"sigma\":     " << setprecision(3) << sigma << ",\n"
       << indent << "\"skewness\":  " << setprecision(3) << skewness << ",\n"
       << indent << "\"kurtosis\":  " << setprecision(3) << kurtosis;
}


ChunkIndex::ChunkIndex() :
    _chunks(),
    _subChunks(),
    _modified(false),
    _chunkStats(),
    _subChunkStats()
{ }

ChunkIndex::ChunkIndex(fs::path const & path) :
    _chunks(),
    _subChunks(),
    _modified(false),
    _chunkStats(),
    _subChunkStats()
{
    _read(path);
}

ChunkIndex::ChunkIndex(vector<fs::path> const & paths) :
    _chunks(),
    _subChunks(),
    _modified(false),
    _chunkStats(),
    _subChunkStats()
{
    typedef vector<fs::path>::const_iterator Iter;
    for (Iter i = paths.begin(), e = paths.end(); i != e; ++i) {
        _read(*i);
    }
}

ChunkIndex::ChunkIndex(ChunkIndex const & idx) :
    _chunks(idx._chunks),
    _subChunks(idx._subChunks),
    _modified(idx._modified),
    _chunkStats(idx._chunkStats),
    _subChunkStats(idx._subChunkStats)
{ }

ChunkIndex::~ChunkIndex() { }

void ChunkIndex::write(fs::path const & path, bool truncate) const {
    size_t numBytes = _subChunks.size()*static_cast<size_t>(ENTRY_SIZE);
    boost::scoped_array<uint8_t> buf(new uint8_t[numBytes]);
    uint8_t * b = buf.get();
    // The file format is simply an array of (sub-chunk ID, counts) pairs.
    for (SubChunkIter i = _subChunks.begin(), e = _subChunks.end();
         i != e; ++i) {
        Entry const & entry = i->second;
        b = encode(b, static_cast<uint64_t>(i->first));
        b = encode(b, entry.numRecords);
        b = encode(b, entry.numOverlapRecords);
    }
    OutputFile f(path, truncate);
    f.append(buf.get(), numBytes);
}


namespace {
    template <typename K> struct EntryPairCmp {
        bool operator()(pair<K, ChunkIndex::Entry> const & p1,
                        pair<K, ChunkIndex::Entry> const & p2) const {
            return p1.first < p2.first;
        }
    };
}

void ChunkIndex::write(ostream & os, int verbosity) const {
    typedef pair<int32_t, Entry> Chunk;
    typedef pair<int64_t, Entry> SubChunk;

    static string const INDENT("\t\t");
    if (_modified) {
        _computeStats();
    }
    os << "{\n"
          "\t\"chunkStats\": {\n";
    _chunkStats.write(os, INDENT);
    os << "\n\t},\n"
          "\t\"overlapChunkStats\": {\n";
    _overlapChunkStats.write(os, INDENT);
    os << "\n\t},\n"
          "\t\"subChunkStats\": {\n";
    _subChunkStats.write(os, INDENT);
    os << "\n\t},\n"
          "\t\"overlapSubChunkStats\": {\n";
    _overlapSubChunkStats.write(os, INDENT);
    os << "\n\t}";
    if (verbosity < 0) {
        os << "\n}";
        return;
    }
    os << ",\n"
          "\t\"chunks\": [\n";
    // Extract and sort non-empty chunks and sub-chunks.
    vector<Chunk> chunks;
    vector<SubChunk> subChunks;
    chunks.reserve(_chunks.size());
    for (ChunkIter c = _chunks.begin(), e = _chunks.end(); c != e; ++c) {
        chunks.push_back(*c);
    }
    sort(chunks.begin(), chunks.end(), EntryPairCmp<int32_t>());
    if (verbosity > 0) {
        subChunks.reserve(_subChunks.size());
        for (SubChunkIter s = _subChunks.begin(), e = _subChunks.end();
             s != e; ++s) {
            subChunks.push_back(*s);
        }
        sort(subChunks.begin(), subChunks.end(), EntryPairCmp<int64_t>());
    }
    // Print out chunk record counts.
    size_t sc = 0;
    for (size_t c = 0; c < chunks.size(); ++c) {
        if (c > 0) {
            os << ",\n";
        }
        int32_t const chunkId = chunks[c].first;
        Entry const * e = &chunks[c].second;
        os << "\t\t{\"id\":  " << setw(5) << chunkId << ", \"nrec\": ["
           << e->numRecords << ", " << e->numOverlapRecords << "]";
        if (verbosity > 0) {
            // Print record counts for sub-chunks of chunkId.
            os << ", \"subchunks\": [\n";
            size_t s = sc;
            for (; s < subChunks.size(); ++s) {
                if ((subChunks[s].first >> 32) != chunkId) {
                    break;
                }
                if (s > sc) {
                    os << ",\n";
                }
                int32_t subChunkId = static_cast<int32_t>(
                    subChunks[s].first & 0xfffffff);
                e = &subChunks[s].second;
                os << "\t\t\t{\"id\":" << setw(5) << subChunkId
                   << ", \"nrec\": [" << e->numRecords
                   << ", " << e->numOverlapRecords << "]}";
            }
            os << "\n\t\t]";
        }
        os << "}";
    }
    os << "\n\t]\n}";
}

void ChunkIndex::add(ChunkLocation const & loc, size_t n) {
    if (n == 0) {
        return;
    }
    Entry * c = &_chunks[loc.chunkId];
    Entry * sc = &_subChunks[_key(loc.chunkId, loc.subChunkId)];
    if (loc.overlap) {
        c->numOverlapRecords += n;
        sc->numOverlapRecords += n;
    } else {
        c->numRecords += n;
        sc->numRecords += n;
    }
    _modified = true;
}

void ChunkIndex::merge(ChunkIndex const & idx) {
    if (this == &idx || idx.empty()) {
        return;
    }
    _modified = true;
    for (ChunkIter c = idx._chunks.begin(), e = idx._chunks.end();
         c != e; ++c) {
        _chunks[c->first] += c->second;
    }
    for (SubChunkIter s = idx._subChunks.begin(), e = idx._subChunks.end();
         s != e; ++s) {
        _subChunks[s->first] += s->second;
    }
}

void ChunkIndex::clear() {
    _chunks.clear();
    _subChunks.clear();
    _modified = false;
    _chunkStats.clear();
    _overlapChunkStats.clear();
    _subChunkStats.clear();
    _overlapSubChunkStats.clear();
}

void ChunkIndex::swap(ChunkIndex & idx) {
    using std::swap;
    if (this != &idx) {
        swap(_chunks, idx._chunks);
        swap(_subChunks, idx._subChunks);
        swap(_modified, idx._modified);
        swap(_chunkStats, idx._chunkStats);
        swap(_overlapChunkStats, idx._overlapChunkStats);
        swap(_subChunkStats, idx._subChunkStats);
        swap(_overlapSubChunkStats, idx._overlapSubChunkStats);
    }
}

ChunkIndex::Entry const ChunkIndex::EMPTY;

// Read array of (sub-chunk ID, counts) pairs from a file, and add each
// count to the in-memory sub-chunk and chunk to count maps
void ChunkIndex::_read(fs::path const & path) {
    InputFile f(path);
    if (f.size() % ENTRY_SIZE != 0) {
        throw runtime_error("Invalid chunk index file.");
    }
    if (f.size() == 0) {
        return;
    }
    boost::scoped_array<uint8_t> data(new uint8_t[f.size()]);
    f.read(data.get(), 0, f.size());
    _modified = true;
    for (uint8_t const * b = data.get(), * e = data.get() + f.size();
         b < e; b += ENTRY_SIZE) {
        int64_t id = static_cast<int64_t>(decode<uint64_t>(b));
        int32_t chunkId = static_cast<int32_t>(id >> 32);
        Entry entry;
        entry.numRecords = decode<uint64_t>(b + 8);
        entry.numOverlapRecords = decode<uint64_t>(b + 16);
        _chunks[chunkId] += entry;
        _subChunks[id] += entry;
    }
}

void ChunkIndex::_computeStats() const {
    if (_chunks.empty()) {
        _chunkStats.clear();
        _overlapChunkStats.clear();
        _subChunkStats.clear();
        _overlapSubChunkStats.clear();
        return;
    }
    vector<uint64_t> counts, overlapCounts;
    counts.reserve(_subChunks.size());
    overlapCounts.reserve(_subChunks.size());
    for (ChunkIter c = _chunks.begin(), e = _chunks.end(); c != e; ++c) {
        counts.push_back(c->second.numRecords);
        overlapCounts.push_back(c->second.numOverlapRecords);
    }
    _chunkStats.computeFrom(counts);
    _overlapChunkStats.computeFrom(overlapCounts);
    counts.clear();
    overlapCounts.clear();
    for (SubChunkIter s = _subChunks.begin(), e = _subChunks.end(); s != e; ++s) {
        counts.push_back(s->second.numRecords);
        overlapCounts.push_back(s->second.numOverlapRecords);
    }
    _subChunkStats.computeFrom(counts);
    _overlapSubChunkStats.computeFrom(overlapCounts);
    _modified = false;
}

}}}} // namespace lsst::qserv::admin::dupr
