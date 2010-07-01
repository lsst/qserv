#ifndef LSST_QSERV_MASTER_SQLSUBSTITUTION_H
#define LSST_QSERV_MASTER_SQLSUBSTITUTION_H

#include <deque>

#include "lsst/qserv/master/ChunkMapping.h"
#include "lsst/qserv/master/Substitution.h"

namespace lsst {
namespace qserv {
namespace master {

class SqlSubstitution {
public:
    typedef StringMapping Mapping;
    typedef std::deque<std::string> Deque;
    typedef Deque::const_iterator DequeConstIter;

    SqlSubstitution(std::string const& sqlStatement, 
                    Mapping const& mapping, 
                    std::string const& defaultDb="");
    
    void importSubChunkTables(char** cStringArr);
    std::string transform(Mapping const& m, int chunk, int subChunk);
    std::string substituteOnly(Mapping const& m);
    
    /// 0: none, 1: chunk, 2: subchunk
    int getChunkLevel() const { return _chunkLevel; }
    bool getHasAggregate() const {return _hasAggregate; }
    std::string getError() const { return _errorMsg; }
    std::string getFixupSelect() const { return _fixupSelect; }
    std::string getFixupPost() const { return _fixupPost; }

private:
    typedef boost::shared_ptr<Substitution> SubstPtr;

    void _build(std::string const& sqlStatement, Mapping const& mapping,
                std::string const& defaultDb);
    void _computeChunkLevel(bool hasChunks, bool hasSubChunks);
    std::string _fixDbRef(std::string const& s, int chunk, int subChunk);

    std::string _delimiter;
    std::string _errorMsg;
    SubstPtr _substitution;
    int _chunkLevel;
    bool _hasAggregate;
    std::string _fixupSelect;
    std::string _fixupPost;
    Deque _subChunked;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_SQLSUBSTITUTION_H

