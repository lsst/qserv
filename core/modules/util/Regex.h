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

#ifndef LSST_QSERV_UTIL_REGEX_H
#define LSST_QSERV_UTIL_REGEX_H

// System headers
#include <cassert>
#include <regex.h>
#include <string>

namespace lsst {
namespace qserv {
namespace util {

class Regex {
public:
    class Match {
    public:
        Match(std::string const& s=std::string()) : _s(s) {}
        std::string const& str(int i) const {
            assert(i==0);  // Can only do one match
            return _s;
        }
        regmatch_t* regMatch() { return &_rmatch; }
        int eo() const { return _rmatch.rm_eo; }
        void updateStr(char const* buffer) {
            _s.assign(buffer, _rmatch.rm_so,
                      _rmatch.rm_eo - _rmatch.rm_so);
        }
    private:
        std::string _s;
        regmatch_t _rmatch;
    };
    class Iterator {
    public:
        Iterator() : _regex(0), _position(-1), _cursor(0) {}
        Iterator(regex_t* regex, std::string const& s)
            : _regex(regex), _s(s), _match(s) {
            setFirst();
        }
        Iterator& operator++() { // pre-increment
            /* substring found between pm.rm_so and pm.rm_eo */
            /* This call to regexec() finds the next match */
            assert(_position >= 0);
            int result = regexec(_regex, _cursor,
                                 1, _match.regMatch(), REG_NOTBOL);
            if(result != 0)
                _position = -1;
            else {
                _match.updateStr(_cursor);
                _position += _match.eo();
                _cursor += _match.eo();
            }
            return *this;
        }
        bool operator==(Iterator const& rhs) const {
            if(_position == -1) return _position == rhs._position;
            return (_position == rhs._position) && (_regex == rhs._regex);
        }
        bool operator!=(Iterator const& rhs) const {
            return !(*this == rhs);
        }
        Match const& operator*() const {
            return _match;
        }
        inline static Iterator const& end() {
            static Iterator nullIterator;
            return nullIterator;
        }
    private:
        void setFirst() {
            _cursor = _s.c_str();
            _position = 0;
            ++(*this);
        }
        regex_t* _regex;
        int _position;
        Match _match;
        std::string _s;
        char const* _cursor;
    };
    Regex(char const* expr) {
        regcomp(&_regex, expr, 0);
    }
    ~Regex() {
        regfree(&_regex);
    }
    Iterator newIterator(std::string const& s) {
        return Iterator(&_regex, s);
    }
private:
    regex_t _regex;
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_REGEX_H
