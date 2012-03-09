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
 
// Std C++
#include <list>

// Boost
#include "boost/shared_ptr.hpp"
#include "boost/format.hpp"
#include "boost/regex.hpp"

// Package
#include "lsst/qserv/master/SqlSubstitution.h"
#include "lsst/qserv/master/SqlParseRunner.h"


namespace qMaster = lsst::qserv::master;

namespace { // Anonymous

// Helper for checking strings
bool endsWith(std::string const& s, char const* ending) {
    std::string::size_type p = s.rfind(ending, std::string::npos);
    std::string::size_type l = std::strlen(ending);
    return p == (s.size() - l);
}

template <class Map>
void printMap(Map const& m) {
    typename Map::const_iterator b = m.begin();
    typename Map::const_iterator e = m.end();
    typename Map::const_iterator i;
    for(i=b; i != e; ++i) {
        std::cout << i->first << " : " << i->second << std::endl;
    }
}

} // Anonymous namespace

qMaster::SqlSubstitution::SqlSubstitution(std::string const& sqlStatement, 
                                          ChunkMapping const& mapping,
                                          qMaster::StringMap const& config)
    : _delimiter("*?*"), _hasAggregate(false), 
      _mapping(mapping), 
      _config(config) {
    // client DB context
    _defaultDb = getFromMap(config, "table.defaultdb", "LSST"); 

    // Note: makes copy of chunkmapping.
    _build(sqlStatement);
}

void qMaster::SqlSubstitution::importSubChunkTables(char const** cStringArr) {
    _subChunked.clear();
    for(int i=0; cStringArr[i]; ++i) {
        std::string s = cStringArr[i];
        _subChunked.push_back(s);
        if(!endsWith(s, "SelfOverlap")) {
            _subChunked.push_back(s + "SelfOverlap");
        }
        if(!endsWith(s, "FullOverlap")) {
            _subChunked.push_back(s + "FullOverlap");
        }        
    }
}
    
std::string qMaster::SqlSubstitution::transform(int chunk, int subChunk) {
    boost::lock_guard<boost::mutex> lock(_mappingMutex);
    StringMap const& m = _mapping.getMapReference(chunk, subChunk);
    //std::cout << "Transforming for " << chunk << " " << subChunk << std::endl;
    if(!_substitution.get()) return std::string();
    return _fixDbRef(_substitution->transform(m), chunk, subChunk);
}

std::string 
qMaster::SqlSubstitution::substituteOnly(qMaster::StringMap const& m) {
    if(!_substitution.get()) return std::string();
    return _substitution->transform(m);
}

void qMaster::SqlSubstitution::_build(std::string const& sqlStatement) {
    boost::lock_guard<boost::mutex> lock(_mappingMutex);
    StringMap const& m = _mapping.getMapReference(999999,999999); 
    std::string template_;

    StringMap::const_iterator end = m.end();
    std::list<std::string> names;
    for(StringMap::const_iterator i=m.begin(); i != end; ++i) {
	names.push_back(i->first);
    }
    //std::cout << "PARSING: " << sqlStatement << std::endl;
    boost::shared_ptr<SqlParseRunner> spr;
    spr = SqlParseRunner::newInstance(sqlStatement, _delimiter, _config);
    spr->setup(names);
    if(spr->getHasAggregate()) {
        template_ = spr->getAggParseResult();
    } else {
        template_ = spr->getParseResult();
    } 
    //std::cout << "Substitution template:: " << template_ << std::endl;
    _computeChunkLevel(spr->getHasChunks(), spr->getHasSubChunks());
    if(template_.empty() || !spr->getError().empty()) {
        _errorMsg = spr->getError();
    } else {
        _substitution = SubstPtr(new Substitution(template_, _delimiter, true));
        _hasAggregate = spr->getHasAggregate();
        _mFixup = spr->getMergeFixup();
    }
}


std::string qMaster::SqlSubstitution::_fixDbRef(std::string const& s, 
                                                int chunk, int subChunk) {

    // # Replace sometable_CC_SS or anything.sometable_CC_SS 
    // # with Subchunks_CC, 
    // # where CC and SS are chunk and subchunk numbers, respectively.
    // # Note that "sometable" is any subchunked table.
    std::string result = s;
    DequeConstIter e = _subChunked.end();
    for(DequeConstIter i=_subChunked.begin(); i != e; ++i) {
        std::string sName = (boost::format("%s_%d_%d") 
                             % *i % chunk % subChunk).str();
        std::string pat = (boost::format("(\\w+\\.)?%s") % sName).str();        
        boost::regex r(pat);
        // FIXME: Forces default DB right now.
        // Ideally, if a db is available, use it,
        // but if not, use the default db.
        std::string sub = (boost::format("Subchunks_%s_%d.%s") 
                           % _defaultDb % chunk % sName).str();
        //std::cout << "sName=" << sName << "  pat=" << pat << std::endl;
        result =  boost::regex_replace(result, r, sub);
        //std::cout << "out=" << result << std::endl;
    }
    return result;

    // for s in slist:
    //     sName = "%s_%d_%d" % (s, chunk, subChunk)
    //     patStr = "(\w+[.])?%s" % sName
    //     sub = "Subchunks_%d.%s" % (chunk, sName)
    //     res = re.sub(patStr, sub, res)
    // return res
}

void 
qMaster::SqlSubstitution::_computeChunkLevel(bool hasChunks, bool hasSubChunks){
    // SqlParseRunner's TableList handler will know if it applied 
    // any subchunk rules, or if it detected any chunked tables.

    if(hasChunks) {
        if(hasSubChunks) {
            _chunkLevel = 2;
        } else {
            _chunkLevel = 1;
        }
    } else {
        _chunkLevel = 0;
    }
}
