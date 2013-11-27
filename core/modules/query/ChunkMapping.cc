/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

// ChunkMapping is a helper class that generates mappings from
// placeholder table references to physical table names.  This
// facilitates substituting physical chunk numbers into a generated
// query without requiring another walk through the original parse
// tree.

#include "query/ChunkMapping.h"
#include "qdisp/ChunkMeta.h"

namespace lsst {
namespace qserv {
namespace query {

ChunkMapping::Map
ChunkMapping::getMapping(int chunk, int subChunk) {
    Map m;
    ModeMap::const_iterator end = _map.end();
    std::string chunkStr = _toString(chunk);
    std::string subChunkStr = _toString(subChunk);
    static const std::string one("1");
    static const std::string two("2");
    // Insert mapping for: plainchunk, plainsubchunk1, plainsubchunk2
    for(ModeMap::const_iterator i = _map.begin(); i != end; ++i) {
        // Object --> Object_chunk
        // Object_so --> ObjectSelfOverlap_chunk
        // Object_fo --> ObjectFullOverlap_chunk
        // Object_s1 --> Object_chunk_subchunk
        // Object_s2 --> Object_chunk_subchunk
        // Object_sso --> ObjectSelfOverlap_chunk_subchunk
        // Object_sfo --> ObjectFullOverlap_chunk_subchunk
        std::string c("_" + chunkStr);
        std::string sc("_" + subChunkStr);
        std::string soc("SelfOverlap_" + chunkStr);
        std::string foc("FullOverlap_" + chunkStr);
        m.insert(MapValue(i->first, i->first + c));
        m.insert(MapValue(i->first + "_so", i->first + soc));
        m.insert(MapValue(i->first + "_fo", i->first + foc));
        if(i->second == CHUNK) {
            // No additional work needed
        } else if (i->second == CHUNK_WITH_SUB) {
            m.insert(MapValue(i->first + _subPrefix + one,
                              i->first + c + sc));
            // Might deprecate the _s2 version in this context
            m.insert(MapValue(i->first + _subPrefix + two,
                              i->first + c + sc));
            m.insert(MapValue(i->first + "_sso",
                              i->first + soc + sc));
            m.insert(MapValue(i->first + "_sfo",
                              i->first + foc + sc));
        }
    }
    return m;
}

ChunkMapping::Map const&
ChunkMapping::getMapReference(int chunk, int subChunk) {
    _instanceMap = getMapping(chunk, subChunk);
    return _instanceMap;
}

void
ChunkMapping::setFromMeta(qdisp::ChunkMeta const& m) {
    qdisp::ChunkMeta::EntryList const& elist = m.getEntries();
    typedef qdisp::ChunkMeta::EntryList::const_iterator Citer;
    Citer begin = elist.begin();
    Citer end = elist.end();
    for(Citer i = begin; i != end; ++i) {
        // Discard i->db since it's not in the mapping.
        // Ignore collisions if tables (of different dbs)
        // have different partitioning.
        if(i->chunkLevel == 1) addChunkKey(i->table);
        else if(i->chunkLevel == 2) addSubChunkKey(i->table);
    }
}

}}} // namespace lsst::qserv::query
