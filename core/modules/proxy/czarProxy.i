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


%module czarProxy
%{
#include "proxy/czarProxy.h"
%}

%include "std_except.i"
%include "std_map.i"
%include "std_string.i"
%include "std_vector.i"

%template(StringVector) std::vector<std::string>;
%template(StringMap) std::map<std::string, std::string>;
%feature("novaluewrapper") StringVector;
%feature("novaluewrapper") StringMap;

// vector of strings is returned as a table
%typemap(out) std::vector<std::string> {
    lua_newtable(L);
    for(unsigned i=0; i < $1.size(); ++i) {
        const std::string& item = $1[i];
        lua_pushlstring(L, item.data(), item.size());
        lua_rawseti(L, -2, i+1);  // -1 is the number, -2 is the table
    }
    SWIG_arg ++;
}

// accept table for string map
%typemap(in, checkfn="lua_istable") std::map<std::string, std::string> const& (std::map<std::string, std::string> temp) {
    /* table is in the stack at index $input */
    lua_pushnil(L);  /* first key */
    while (lua_next(L, $input) != 0) {
        /* uses 'key' (at index -2) and 'value' (at index -1) */
        if(!(lua_isstring(L, -2) && lua_isstring(L, -1))) {
            SWIG_fail_arg("lsst::qserv::proxy::submitQuery",2,"std::map< std::string,std::string > const &");
        }
        size_t lenKey, lenVal;
        const char* key = lua_tolstring(L, -2, &lenKey);
        const char* val = lua_tolstring(L, -1, &lenVal);
        temp.insert(std::make_pair(std::string(key, lenKey), std::string(val, lenVal)));
        /* removes 'value'; keeps 'key' for next iteration */
        lua_pop(L, 1);
    }
    $1 = &temp;
}

%include "proxy/czarProxy.h"
