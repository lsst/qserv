// -*- LSST-C++ -*-
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

#ifndef LSST_QSERV_UTIL_SUBSTITUTION_H
#define LSST_QSERV_UTIL_SUBSTITUTION_H

// System headers
#include <map>
#include <string>
#include <vector>

namespace lsst {
namespace qserv {
namespace util {

/// class Substitution : Simply performs substitution on a template
/// string using a supplied string-to-string mapping.  Optimized for
/// repeated substitution using different mappings.
class Substitution {
public:
    typedef std::pair<std::string, std::string> StringPair;
    typedef std::map<std::string, std::string> Mapping;
    typedef std::vector<StringPair> MapVector;
    typedef std::vector<std::string> StringVector;

    Substitution(std::string template_, std::string const& delim, bool shouldFinalize=true);

    std::string transform(Mapping const& m);

private:
    struct Item { //where subs are needed and how big the placeholders are.
        std::string name;
        int position;
        int length;
    };

    template<typename Iter>
    inline unsigned _max(Iter begin, Iter end) {
        unsigned m=0;
        for(Iter i = begin; i != end; ++i) {
            if(m < i->size()) m = i->size();
        }
        return m;
    }

    void _build(std::string const& delim);

    std::vector<Item> _index;
    std::string _template;
    bool _shouldFinalize;
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_SUBSTITUTION_H

