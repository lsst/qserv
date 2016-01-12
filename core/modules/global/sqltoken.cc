// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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
/**
  * @file
  *
  * @brief Utility functions for working with SQL tokens.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "global/sqltoken.h"

// System headers
#include <set>

// Third-party headers
#include "boost/algorithm/string/predicate.hpp"

namespace { // File-scope helpers
struct InsensitiveCompare {
    bool operator() (std::string const& a, std::string const& b) const {
        return boost::ilexicographical_compare(a,b);
    }
};

/// std::map-based lookup of separating words.
/// If this becomes significant in execution time, consider faster
/// implementations. Possibilities: hashing, lookup tables.
struct CompareMap {
    CompareMap() {
        const char* sepWords[] = {"select", "from", "where", "by", "limit", "and", "or"};
        const int swSize=7;
        _sepWords.insert(sepWords, sepWords + swSize);
    }
    inline bool isSeparatingWord(std::string const& w) {
        return _sepWords.find(w) != _sepWords.end();
    };
    std::set<std::string, InsensitiveCompare> _sepWords;
};
CompareMap _cMap;

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace sql {

bool
sqlShouldSeparate(std::string const& s, int last, int next) {
    if (_cMap.isSeparatingWord(s)) return true;
    bool lastAlnum = isalnum(last);
    bool nextAlnum = isalnum(next);
    return (lastAlnum && nextAlnum) // adjoining alnums
        || ((last == '\'') && nextAlnum) // 'saf
        || ((next == '\'') && lastAlnum) // saf'
        || ((last == '*') && nextAlnum) // *saf
        || ((next == '*') && lastAlnum) // saf*
        || ((last == ')') && nextAlnum) // )asdf
        || ((last == '#') && nextAlnum) // #asdf
        || ((last == '%') && nextAlnum) // %asdf
        || ((next == '%') && lastAlnum) // asdf%
        || ((last == '_') && nextAlnum) // _asdf
        || ((next == '_') && lastAlnum) // asdf_
        ;
}

}}} // namespace lsst::qserv::sql
