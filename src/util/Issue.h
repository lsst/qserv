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
#ifndef LSST_QSERV_UTIL_ISSUE_H
#define LSST_QSERV_UTIL_ISSUE_H

// System headers
#include <exception>
#include <iosfwd>
#include <string>

// evil macro
#define ERR_LOC lsst::qserv::util::Issue::Context(__FILE__, __LINE__, __func__)

namespace lsst {
namespace qserv {
namespace util {

/// @addtogroup util

/**
 *  @ingroup util
 *
 *  @brief Base class for other error classes.
 *
 *  This class inherits from standard exception class and adds the facility
 *  for tracking of where the exception originated.
 *
 *  Typical use:
 *
 *      class MyException : public util::Issue {
 *      public:
 *          MyException(util::Issue::Context const& ctx, std::string const& message)
 *              : util::Issue(ctx, "MyException: " + message)
 *          {}
 *      };
 *
 *      // ... throw exception
 *      throw MyException(ERR_LOC, "something exceptional happened");
 *
 *  Catching site will see something like from the exception what() method:
 *  "MyException: something exceptional happened [in function test() at test.cpp:42]"
 */

class Issue : public std::exception {
public:

    /// Context defines where exception has happened.
    class Context {
    public:
        // Constructor takes location of the context
        Context(char const* file, int line, char const* func);
        void print(std::ostream& out) const;
    private:
        std::string _file;
        std::string _func;
        int _line;
    };

    /// Constructor takes issue location and a message.
    Issue(Context const& ctx, std::string const& message);

    // Destructor
    virtual ~Issue() throw();

    /// Implements std::exception::what(), returns message and
    /// context in one string.
    virtual char const* what() const throw();

    /// Returns original message (without context).
    std::string const& message() const { return _message; }

private:

    // Data members
    std::string _message;
    std::string _fullMessage;  /// Message string plus context

};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_ISSUE_H
