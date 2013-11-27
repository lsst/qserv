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

#ifndef LSST_QSERV_UTIL_FORMAT_H
#define LSST_QSERV_UTIL_FORMAT_H

#include <string>
#include <vector>
#include <regex.h>
#include <assert.h>

namespace lsst {
namespace qserv {
namespace util {

    inline unsigned char hexChar(unsigned char i) {
        if(i < 10) return '0' + i;
        else return (i-10) + 'a';
    }
    inline std::string hashFormat(unsigned char const* hashVal, int length) {
        unsigned char str[length*2];
        int pos=0;
        for (int i = 0; i < length; ++i) {
            str[pos++] = hexChar(hashVal[i] >> 4); // upper 4 bits
            str[pos++] = hexChar(hashVal[i] & 15); // lower 4 bits
        }
        return std::string((char*)str, length*2);
    }


class PosFormat {
public:
    PosFormat(std::string const& f) : _formatStr(f) {
    }
    template <typename T>
    std::string convert(T const& i) {
        std::ostringstream os;
        os << i;
        return os.str();
    }

    template <typename T>
    PosFormat& operator%(T const& sub) {
        // Add to lookup table.
        std::string s = convert(sub);
        _subs.push_back(s);
        return *this;
    }

    std::string str() const {
        std::string result;
        std::vector<std::string> outputs;
        int sp = 0;
        int ep = 0;
        int state = PLAIN;
        int total=0;
        for(int pos=0; pos < _formatStr.length(); ++pos) {
            char x = _formatStr[pos];
            switch(x) {
            case '%':
                switch(state) {
                case PLAIN: // Start tracking ref
                    if(sp != ep) {
                        outputs.push_back(std::string(_formatStr, sp, ep-sp));
                    }
                    state = REF;
                    break;
                case REF: // Stop tracking ref
                    if(sp == ep) { // Double %%: emit %, continue
                        outputs.push_back(std::string("%"));
                    } else {
                        int refnum = atoi(std::string(_formatStr,sp,ep-sp));
                        assert(refnum > 0);
                        assert(refnum <= _subs.size());
                        outputs.push_back(_subs[refnum-1]);
                        state = PLAIN;
                    }
                    break;
                default: // Invalid state
                    assert(0);
                }
                sp = ep = pos+1;

            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                ep = pos+1;
                break;
            default:
                assert(state == PLAIN);
                ep = pos+1;
                break;
            }
        }
        if(sp != ep) {
            outputs.push_back(std::string(_formatStr, sp, ep-sp));
        }
        for(int i=0; i < outputs.size(); ++i) {
            result += outputs[i];
        }
        return result;
    }
private:
    int atoi(std::string const& s) const {
        std::istringstream is(s);
        int r;
        is >> r;
        return r;
    }
    enum states {PLAIN, REF};
    std::string _formatStr;
    std::vector<std::string> _subs;
};

std::ostream& operator<<(std::ostream& os, PosFormat const& pf) {
    os << pf.str();
    return os;
}

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_FORMAT_H
