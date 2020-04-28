// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_PARSER_PARSEEXCEPTION_H
#define LSST_QSERV_PARSER_PARSEEXCEPTION_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <stdexcept>
#include <map>

// Qserv headers
#include "global/Bug.h"


namespace lsst {
namespace qserv {
namespace parser {

/// ParseException is a trivial exception for Qserv parse problems.
/// ParseExceptions automatically retrieves basic information from the ANTLR
/// parse node to be bundled with the exception for greater context.
class ParseException : public std::runtime_error {
public:
    ~ParseException() override = default;

    /// Parse exception with a qserv "likely bug" (see global/Bug.h)
    explicit ParseException(Bug const& b);

    /// Parse related exception where the antlr4 context need not be included.
    explicit ParseException(std::string const& msg);
};


// antlr4 parse exception; this may be raised during listening if there is an error in the enter/exit
// functions. It may happen because an unanticipated SQL statement was entered into qserv and we don't yet
// have the proper handling for it set up yet.
class adapter_order_error : public ParseException {
public:
    using ParseException::ParseException;
};


// antlr4 parse exception; thrown in the case of unexpected events during the parse.
class adapter_execution_error : public ParseException {
public:
    using ParseException::ParseException;
};


}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_PARSEEXCEPTION_H
