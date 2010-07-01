#ifndef LSST_QSERV_MASTER_CHUNKMAPPING_H
#define LSST_QSERV_MASTER_CHUNKMAPPING_H

// C++ 
#include <map>
#include <string>
#include <sstream>
// Boost
#include "boost/shared_ptr.hpp"

namespace lsst {
namespace qserv {
namespace master {

typedef std::map<std::string, std::string> StringMapping;
// class StringMapping 
// A placeholder that SWIG can translate into a Python dict.
// class StringMapping : public std::map<std::string,std::string> {
// public:
//     StringMapping(); 
// };

class ChunkMapping {
public:
    typedef StringMapping Map;
    typedef Map::value_type MapValue;
    
    ChunkMapping() :_subPrefix("_sc") {}
    
    // Get a Mapping object
    Map getMapping(int chunk, int subChunk);

    // Get a reference to this instance's Mapping, which is overwritten
    // each time this method is called.
    Map const& getMapReference(int chunk, int subChunk);

    // ChunkKeys: tables partitioned into chunks (not subc)
    // SubChunkKeys: tables partitioned into chunks and subchunks.
    void addChunkKey(std::string const& key) { _map[key] = CHUNK; }
    void addSubChunkKey(std::string const& key) { _map[key] = CHUNK_WITH_SUB; }
private:
    enum Mode {UNKNOWN, CHUNK, CHUNK_WITH_SUB};
    typedef std::map<std::string, Mode> ModeMap;
    typedef ModeMap::value_type ModeMapValue;

    template <typename T>
    std::string _toString(T const& t) {
	std::ostringstream oss;
	oss << t;
	return oss.str();
    }

    ModeMap _map;
    Map _instanceMap;
    std::string _subPrefix;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_CHUNKMAPPING_H
