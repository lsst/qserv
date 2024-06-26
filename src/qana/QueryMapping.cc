// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2017 AURA/LSST.
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

// Class header
#include "qana/QueryMapping.h"

// System headers
#include <deque>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "qproc/ChunkSpec.h"
#include "query/QueryTemplate.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.query.QueryMapping");

}

namespace lsst::qserv::qana {

std::string const replace(std::string const& s, std::string const& pat, std::string const& value) {
    std::string result;
    size_t i = 0;
    result.reserve(s.size() + value.size());
    while (true) {
        size_t j = s.find(pat, i);
        result.append(s, i, j - i);
        if (j == std::string::npos) {
            break;
        }
        result.append(value);
        i = j + pat.size();
    }
    return result;
}

class Mapping : public query::QueryTemplate::EntryMapping {
public:
    Mapping(QueryMapping::ParameterMap const& m, qproc::ChunkSpec const& s)
            : _subChunks(s.subChunks.begin(), s.subChunks.end()) {
        _chunkString = boost::lexical_cast<std::string>(s.chunkId);
        if (!_subChunks.empty()) {
            _subChunkString = boost::lexical_cast<std::string>(_subChunks.front());
        }
        _initMap(m);
    }
    Mapping(QueryMapping::ParameterMap const& m, qproc::ChunkSpecSingle const& s) {
        _chunkString = boost::lexical_cast<std::string>(s.chunkId);
        _subChunkString = boost::lexical_cast<std::string>(s.subChunkId);
        _subChunks.push_back(s.subChunkId);
        _initMap(m);
    }
    virtual ~Mapping() {}

    query::QueryTemplate::Entry::Ptr mapEntry(query::QueryTemplate::Entry const& e) const override {
        auto newE = std::make_shared<query::QueryTemplate::StringEntry>(e.getValue());

        // FIXME see if this works
        // if (!e.isDynamic()) {return newE; }

        for (auto i = _replaceItems.begin(); i != _replaceItems.end(); ++i) {
            newE->s = replace(newE->s, i->pattern, i->target);
        }
        return newE;
    }
    bool valid() const {
        return _subChunkString.empty() || (!_subChunkString.empty() && !_subChunks.empty());
    }

private:
    inline void _initMap(QueryMapping::ParameterMap const& m) {
        QueryMapping::ParameterMap::const_iterator i;
        for (i = m.begin(); i != m.end(); ++i) {
            _replaceItems.push_back(MapTuple(i->first, lookup(i->second), i->second));
        }
    }

    inline std::string lookup(QueryMapping::Parameter const& p) const {
        switch (p) {
            case QueryMapping::INVALID:
                return "INVALID";
            case QueryMapping::CHUNK:
                return CHUNK_TAG;
            case QueryMapping::HTM1:
                throw std::range_error("HTM unimplemented");
            default:
                throw std::range_error("Unknown mapping parameter");
        }
    }
    void _nextSubChunk() {
        _subChunks.pop_front();
        if (_subChunks.empty()) return;
        _subChunkString = boost::lexical_cast<std::string>(_subChunks.front());
    }

    std::string _chunkString;
    std::string _subChunkString;
    std::deque<int> _subChunks;

    struct MapTuple {
        MapTuple(std::string const& pattern, std::string const& target, QueryMapping::Parameter p)
                : pattern(pattern), target(target), paramType(p) {}
        std::string pattern;
        std::string target;
        QueryMapping::Parameter paramType;
    };

    std::deque<MapTuple> _replaceItems;
};

////////////////////////////////////////////////////////////////////////
// class QueryMapping implementation
////////////////////////////////////////////////////////////////////////
QueryMapping::QueryMapping() {}

std::string QueryMapping::apply(qproc::ChunkSpec const& s, query::QueryTemplate const& t) const {
    Mapping m(_subs, s);
    std::string str = t.generate(m);
    return str;
}

std::string QueryMapping::apply(qproc::ChunkSpecSingle const& s, query::QueryTemplate const& t) const {
    Mapping m(_subs, s);
    std::string str = t.generate(m);
    return str;
}

bool QueryMapping::hasParameter(Parameter p) const {
    ParameterMap::const_iterator i;
    for (i = _subs.begin(); i != _subs.end(); ++i) {
        if (i->second == p) return true;
    }
    return false;
}

}  // namespace lsst::qserv::qana
