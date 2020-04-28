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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "util/Issue.h"

// System headers
#include <sstream>


namespace lsst {
namespace qserv {
namespace util {

Issue::Context::Context(char const* file, int line, char const* func)
    : _file(file), _func(func), _line(line) {
}

void
Issue::Context::print(std::ostream& out) const {
    out << "in function " << _func << " at " << _file << ":" << _line;
}

Issue::Issue(Context const& ctx, std::string const& message)
    : _message(message), _fullMessage() {
    std::ostringstream str ;
    str << message << " [";
    ctx.print(str);
    str << ']';
    _fullMessage = str.str();
}

// Destructor
Issue::~Issue() throw() {
}

// Implements std::exception::what()
char const*
Issue::what() const throw() {
  return _fullMessage.c_str();
}

}}} // namespace lsst::qserv::util
