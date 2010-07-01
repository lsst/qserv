
#include "lsst/qserv/master/ChunkMapping.h"
namespace qMaster = lsst::qserv::master;

qMaster::ChunkMapping::Map qMaster::ChunkMapping::getMapping(int chunk, int subChunk) {
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

qMaster::ChunkMapping::Map const& qMaster::ChunkMapping::getMapReference(int chunk, int subChunk) {
    _instanceMap = getMapping(chunk, subChunk);
    return _instanceMap;
}
