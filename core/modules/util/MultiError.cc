// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#include "util/MultiError.h"

// System headers
#include <algorithm>
#include <iterator>
#include <sstream>

namespace lsst {
namespace qserv {
namespace util {

std::string const MultiError::HEADER_MSG = "Error(s):\n";

std::string MultiError::toString() const {
    std::ostringstream oss;

    oss << *this;
    return oss.str();
}

std::string MultiError::toOneLineString() const {
    std::ostringstream oss;
    if (!this->empty()) {
          if (this->size()>1) {
              std::ostream_iterator<Error> string_it(oss, ", ");
              std::copy(this->begin(), this->end()-1, string_it);
          }
          oss << this->back();
    }
    return oss.str();
}

bool MultiError::empty() const {
    return errorVector.empty();
}

std::vector<Error>::size_type MultiError::size() const{
    return errorVector.size();
}

std::vector<Error>::const_iterator MultiError::begin() const {
    return errorVector.begin();
}

std::vector<Error>::const_iterator MultiError::end() const{
    return errorVector.end();
}

std::vector<Error>::const_reference MultiError::back() const{
    return errorVector.back();
}

void MultiError::push_back (const std::vector<Error>::value_type& val) {
    errorVector.push_back(val);
}

std::ostream& operator<<(std::ostream &out, MultiError const& multiError) {
    if (!multiError.empty()) {
          out << MultiError::HEADER_MSG << "\t";
          if (multiError.size()>1) {
              std::ostream_iterator<Error> string_it(out, "\n\t");
              std::copy(multiError.begin(), multiError.end()-1, string_it);
          }
          out << multiError.back();
      }
    return out;
}

}}} // lsst::qserv::util
