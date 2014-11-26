// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

// Third-party headers
#include "boost/shared_ptr.hpp"
#include "boost/make_shared.hpp"
#include "boost/thread.hpp"

// Local headers
#include "global/ConfigError.h"

using lsst::qserv::ConfigError;
using lsst::qserv::IntSet;

namespace {
// Workaround lack of copy_if before C++11
template<class InputIt, class OutputIt, class UnaryPredicate>
OutputIt copy_if(InputIt first, InputIt last,
                 OutputIt d_first, UnaryPredicate pred)
{
    return std::remove_copy_if(first, last, d_first, std::not1(pred));
}

std::string makeDefaultFilename() {
    return std::string("emptyChunks.txt");
}

bool isSafe(std::string::value_type const& c) {
    std::locale loc;
    if(std::isalnum(c)) { // Not sure that using the default locale is safe.
        return true;
    }
    switch(c) { // Special cases. '_' is the only one right now.
    case '_':
        return true;
    default:
        return false;
    }
}
struct isSafePred {
    inline bool operator()(std::string::value_type const& c) const {
        return isSafe(c);
    }
    typedef std::string::value_type argument_type;
};

std::string sanitizeName(std::string const& name) {
    std::string out;
#if 0
    copy_if(
        name.begin(), name.end(),
        std::insert_iterator<std::string>(out, out.begin()),
        isSafePred());
#else
    std::remove_copy_if(name.begin(), name.end(),
                        std::insert_iterator<std::string>(out, out.begin()),
                        std::not1(isSafePred()));

#endif
    return out;
}

std::string makeFilename(std::string const& db) {
    return "empty_" + sanitizeName(db) + ".txt";
}

void populate(IntSet& s, std::string const& db) {
    std::ifstream rawStream(makeFilename(db).c_str());
    if(!rawStream.good()) { // On error, try using default filename
        rawStream.close();
        rawStream.open(makeDefaultFilename().c_str());
    }
    if(!rawStream.good()) {
        throw ConfigError("No such empty chunks file: " + makeFilename(db)
                          + " or " + makeDefaultFilename());
    }
    std::istream_iterator<int> chunkStream(rawStream);
    std::istream_iterator<int> eos;
    std::copy(chunkStream, eos, std::insert_iterator<IntSet>(s, s.begin()));
}
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace css {

boost::shared_ptr<IntSet const> EmptyChunks::getEmpty(std::string const& db) {
    boost::lock_guard<boost::mutex> lock(_setsMutex);
    IntSetMap::const_iterator i = _sets.find(db);
    if(i != _sets.end()) {
        IntSetConstPtr readOnly = i->second;
        return readOnly;
    }
    IntSetPtr newSet = boost::make_shared<IntSet>();
    _sets.insert(IntSetMap::value_type(db, newSet));
    populate(*newSet, db); // Populate reference
    return IntSetConstPtr(newSet);
}

bool EmptyChunks::isEmpty(std::string const& db, int chunk) {
    IntSetConstPtr s = getEmpty(db);
    return s->end() == s->find(chunk);
}

}}} // namespace lsst::qserv::css
