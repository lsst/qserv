/*
 * LSST Data Management System
 * Copyright 2009-2013 LSST Corporation.
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
  * @file SqlFragmenter.cc
  *
  * @brief SqlFragmenter breaks up a single string containing several
  * SQL statements into one or more fragments, in the effort to avoid
  * MySQL protocol limits for submitted query length.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "sql/SqlFragmenter.h"

namespace qWorker = lsst::qserv::worker;

// Constants
const std::string qWorker::SqlFragmenter::_delimiter = ";\n";

qWorker::SqlFragmenter::SqlFragmenter(std::string const& query)
    : _query(query),
      _pNext(0),
      _qEnd(query.length()),
      _sizeTarget(1024), // too little?
      _count(0)
{}

qWorker::SqlFragmenter::Piece const& qWorker::SqlFragmenter::getNextPiece() {
    if(_pNext == _qEnd) {
        _current.first = 0;
        return _current;
    }
    _advance();
    return _current;
}

void qWorker::SqlFragmenter::_advance() {
    std::string::size_type begin = _pNext;
    std::string::size_type end;
    std::string::size_type searchTarget;
    searchTarget = begin + _sizeTarget;
    if(searchTarget < _qEnd) {  // Is it worth splitting?
        end = _query.rfind(_delimiter, searchTarget);

        // Did we find a split-point?
        if((end > begin) && (end != std::string::npos)) {
            end += _delimiter.size();
        } else {
            // Look forward instead of backward.
            end = _query.find(_delimiter, begin + _sizeTarget);
            if(end != std::string::npos) { // Found?
                end += _delimiter.size();
            } else { // Not found bkwd/fwd. Use end.
                end = _qEnd;
            }
        }
    } else { // Remaining is small. Don't split further.
        end = _qEnd;
    }
    // Backoff whitepace or null.
    int pos = end;
    char c = _query[pos];
    while((c == '\0') || (c == '\n')
          || (c == ' ') || (c == '\t')) { c = _query[--pos];}
    // Watch out for queries not terminated by semicolon.
    if(c!= ';') {++pos;} // A non-semicolon, non-whitespace-->valuable.

    if (pos > (int)begin) {
        // create piece:
        _current.first = _query.data() + begin;
        _current.second = pos - (int)begin;
    }
    _pNext = end; // Advance for next iteration
    // Catch empty strings.
    if(_current.second && _current.first[0] != '\0') {
        ++_count;
    } else {
        _advance();
    }
}

