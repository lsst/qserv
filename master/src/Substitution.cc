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
/// See Substitution.h
 
// C++
#include <map>
#include <vector>

// Package
#include "lsst/qserv/master/Substitution.h"
#include "lsst/qserv/Logger.h"

namespace qMaster = lsst::qserv::master;

qMaster::Substitution::Substitution(std::string template_, 
                                    std::string const& delim, 
                                    bool shouldFinalize) 
    : _template(template_), _shouldFinalize(shouldFinalize) {
    _build(delim);
}
    
std::string qMaster::Substitution::transform(Mapping const& m) {
    // This can be made more efficient by pre-sizing the result buffer
    // copying directly into it, rather than creating
    // intermediate string objects and appending.
    //
    unsigned pos = 0;
    std::string result;
    // No re-allocations if transformations are constant-size.
    result.reserve(_template.size()); 

#if 0
    for(Mapping::const_iterator i = m.begin(); i != m.end(); ++i) {
        LOGGER_INF << "mapping " << i->first << " " << i->second << std::endl;
    }
#endif
    for(std::vector<Item>::const_iterator i = _index.begin();
        i != _index.end(); ++i) {
        // Copy bits since last match
        result += _template.substr(pos, i->position - pos);
        // Copy substitution
        Mapping::const_iterator s = m.find(i->name);
        if(s == m.end()) {
            result += i->name; // passthrough.
        } else {
            result += s->second; // perform substitution
        }
        // Update position
        pos = i->position + i->length;
    }
    // Copy remaining.
    if(pos < _template.length()) {
        result += _template.substr(pos);
    }
    return result;
}

// Let delim = ***
// blah blah ***Name*** blah blah
//           |         |
//         pos       endpos
//           |-length--|
//        name = Name
void qMaster::Substitution::_build(std::string const& delim) {
    //int maxLength = _max(names.begin(), names.end());
    int delimLength = delim.length();
    for(unsigned pos=_template.find(delim); 
        pos < _template.length(); 
        pos = _template.find(delim, pos+1)) {
        unsigned endpos = _template.find(delim, pos + delimLength);
        Item newItem;
        if(_shouldFinalize) {
            newItem.position = pos;	
            newItem.length = (endpos - pos) + delimLength;
        } else {
            newItem.position = pos + delimLength;
            newItem.length = endpos - pos - delimLength;
        }
        newItem.name.assign(_template, pos + delimLength,
                            endpos - pos - delimLength);
        // Note: length includes two delimiters.
        _index.push_back(newItem);
        pos = endpos;

        // Sanity check:
        // Check to see if the name is in names.	    
    }
}
