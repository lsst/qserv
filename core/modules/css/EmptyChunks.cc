// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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

#include "css/EmptyChunks.h"

// System headers
#include <algorithm>
#include <iterator>
#include <fstream>
#include <functional>
#include <memory>

// Third-party headers
#include "boost/thread.hpp"

// Qserv headers
#include "global/ConfigError.h"
#include "global/stringUtil.h"

using lsst::qserv::ConfigError;
using lsst::qserv::IntSet;

namespace {

std::string
makeFilename(std::string const& db) {
    return "empty_" + lsst::qserv::sanitizeName(db) + ".txt";
}

void
populate(std::string const& path,
         std::string const& fallbackFile,
         IntSet& s,
         std::string const& db) {
    std::string best = path + "/" + makeFilename(db);
    std::ifstream rawStream(best.c_str());
    if(!rawStream.good()) { // On error, try using default filename
        rawStream.close();
        rawStream.open(fallbackFile.c_str());
    }
    if(!rawStream.good()) {
        throw ConfigError("No such empty chunks file: " + best
                          + " or " + fallbackFile);
    }
    std::istream_iterator<int> chunkStream(rawStream);
    std::istream_iterator<int> eos;
    std::copy(chunkStream, eos, std::insert_iterator<IntSet>(s, s.begin()));
}
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace css {

std::shared_ptr<IntSet const>
EmptyChunks::getEmpty(std::string const& db) const {
    boost::lock_guard<boost::mutex> lock(_setsMutex);
    IntSetMap::const_iterator i = _sets.find(db);
    if(i != _sets.end()) {
        IntSetConstPtr readOnly = i->second;
        return readOnly;
    }
    IntSetPtr newSet = std::make_shared<IntSet>();
    _sets.insert(IntSetMap::value_type(db, newSet));
    populate(_path, _fallbackFile, *newSet, db); // Populate reference
    return IntSetConstPtr(newSet);
}

bool
EmptyChunks::isEmpty(std::string const& db, int chunk) const {
    IntSetConstPtr s = getEmpty(db);
    return s->end() != s->find(chunk);
}

}}} // namespace lsst::qserv::css
