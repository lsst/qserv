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
 
#ifndef LSST_QSERV_MASTER_SQLSUBSTITUTION_H
#define LSST_QSERV_MASTER_SQLSUBSTITUTION_H

#include <deque>

#include <boost/thread.hpp>

#include "lsst/qserv/master/ChunkMapping.h"
#include "lsst/qserv/master/Substitution.h"
#include "lsst/qserv/master/mergeTypes.h"
#include "lsst/qserv/master/common.h"

namespace lsst {
namespace qserv {
namespace master {

/// class SqlSubstitution : From an existing SQL statement, parse and
/// generate subqueries according for supplied chunk and subchunk numbers.
/// Uses SqlParseRunner for parsing and Substitution to perform the text
/// substitution.
class SqlSubstitution {
public:
    typedef std::deque<std::string> Deque;
    typedef Deque::const_iterator DequeConstIter;
    typedef lsst::qserv::master::MergeFixup MergeFixup;

    // Non-typedef'd map is necessary for SWIG.
    SqlSubstitution(std::string const& sqlStatement, 
                    ChunkMapping const& mapping, 
                    std::map<std::string,std::string> const& config);
    /// config should include qserv master config + current session context
    /// i.e., defaultDb=LSST (or defaultDb=TestDb)

    void importSubChunkTables(char** cStringArr);
    std::string transform(int chunk, int subChunk);
    std::string substituteOnly(std::map<std::string, std::string> const& m);
    
    /// 0: none, 1: chunk, 2: subchunk
    int getChunkLevel() const { return _chunkLevel; }
    bool getHasAggregate() const {return _hasAggregate; }
    std::string getError() const { return _errorMsg; }
    std::string getFixupSelect() const { return _mFixup.select; }
    std::string getFixupPost() const { return _mFixup.post; }
    MergeFixup const& getMergeFixup() const { return _mFixup; }

private:
    typedef boost::shared_ptr<Substitution> SubstPtr;

    void _build(std::string const& sqlStatement);
    void _computeChunkLevel(bool hasChunks, bool hasSubChunks);
    std::string _fixDbRef(std::string const& s, int chunk, int subChunk);

    std::string _delimiter;
    std::string _errorMsg;
    SubstPtr _substitution;
    int _chunkLevel;
    bool _hasAggregate;
    MergeFixup _mFixup;
    Deque _subChunked;
    std::string _defaultDb;
    ChunkMapping _mapping;
    boost::mutex _mappingMutex;
    StringMap _config;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_SQLSUBSTITUTION_H

