/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/CmdParser.h"

// System headers
#include <algorithm>
#include <iostream>
#include <stdexcept>

// Qserv headers

namespace lsst {
namespace qserv {
namespace replica {

bool
CmdParser::found_in (std::string const&              val,
                     std::vector<std::string> const& col) {
    return col.end() != std::find(col.begin(), col.end(), val);
}

CmdParser::CmdParser (int                argc,
                      const char* const* argv,
                      const char*        usage)
    :   _usage(usage) {

    _usage = _usage +
    "\nSpecial options:\n"
    "  --help  - print the help page\n";

    for (int i=0; i < argc; ++i) _argv.push_back(argv[i]);
    parse();
}

bool
CmdParser::flag (std::string const& name) const {
    if (name == "help") {
        std::cerr
            << _usage << std::endl;
        return true;
    }
    return _flag.count(name);
}

std::string
CmdParser::parameterRestrictedBy (unsigned int                    pos,
                                  std::vector<std::string> const& allowedValues) const {

    const std::string str = parameter<std::string>(pos);
    if (found_in(str, allowedValues)) return str;

    std::cerr
        << "CmdParser::parameterRestrictedBy(" << std::to_string(pos)
        << "): parameter value is not permitted: " << str << "\n"
        << _usage << std::endl;

    throw std::invalid_argument (
        "CmdParser::parameterRestrictedBy(" + std::to_string(pos) +
        "): parameter value is not permitted: " + str);
}

bool
CmdParser::optionImpl (std::string const& name,
                       bool        const& defaultValue) const {

    std::string const str = optionImpl(name, std::string());
    if (str.empty()) return defaultValue;

    if      (str == "true")  return true;
    else if (str == "false") return false;

    throw std::invalid_argument (
        "CmdParser::optionImpl<bool>: failed to parse a value of option: " + name);
}

int
CmdParser::optionImpl (std::string const& name,
                       int         const& defaultValue) const {

    std::string const str = optionImpl(name, std::string());
    if (str.empty()) return defaultValue;

    try {
        return std::stoi(str);
    } catch (std::exception const&) {
        throw std::invalid_argument (
            "CmdParser::optionImpl<int>: failed to parse a value of option: " + name);
    }
}

unsigned int
CmdParser::optionImpl (std::string const&  name,
                       unsigned int const& defaultValue) const {

    std::string const str = optionImpl(name, std::string());
    if (str.empty()) return defaultValue;

    try {
        return std::stoul(str);
    } catch (std::exception const&) {
        throw std::invalid_argument (
            "CmdParser::optionImpl<uint>: failed to parse a value of option: " + name);
    }
}

std::string
CmdParser::optionImpl (std::string const& name,
                       std::string const& defaultValue) const {
    return _option.count(name) ? _option.at(name) : defaultValue;
}

void
CmdParser::parameterImpl (unsigned int pos,
                          bool&        val) const {
    std::string str;
    parameterImpl(pos, str);

    if      (str == "true")  { val = true;  return; }
    else if (str == "false") { val = false; return; }

    throw std::invalid_argument (
        "CmdParser::parameterImpl<bool>(" + std::to_string(pos) +
        "): failed to parse a value of argument: " + str);
}

void
CmdParser::parameterImpl (unsigned int  pos,
                          int&          val) const {
    std::string str;
    parameterImpl(pos, str);
    
    try {
        val = std::stoi(str);
        return;
    } catch (std::exception const&) {
        throw std::invalid_argument (
            "CmdParser::parameterImpl<int>(" + std::to_string(pos) +
            "): failed to parse a value of argument: " + str);
    }
}

void
CmdParser::parameterImpl (unsigned int  pos,
                          unsigned int& val) const {
    std::string str;
    parameterImpl(pos, str);
    
    try {
        val = std::stoul(str);
        return;
    } catch (std::exception const&) {
        throw std::invalid_argument (
            "CmdParser::parameterImpl<uint>(" + std::to_string(pos) +
            "): failed to parse a value of argument: " + str);
    }
}
void
CmdParser::parameterImpl (unsigned int pos,
                          std::string& val) const {
    if (pos >= _parameter.size()) {
        std::cerr
            << "CmdParser::parameterImpl<string>(" << pos << "): too few positional arguments\n"
            << _usage << std::endl;
        throw std::out_of_range (
            "CmdParser::parameterImpl<string>(" + std::to_string(pos) + "): too few positional arguments");
    }
    val = _parameter[pos];
    return;
}

void
CmdParser::dump (std::ostream& os) const {

    os << "CmdParser::dump()\n";

    os << "  PARAMETERS:\n";
    for (auto const& p: _parameter)
        os << "    " << p << "\n";

    os << "  OPTIONS:\n";
    for (auto const& p: _option)
        os << "    " << p.first << "=" << p.second << "\n";

    os << "  FLAGS:\n";
    for (auto const& p: _flag)
        os << "    " << p << "\n";
}

void
CmdParser::parse () {
    for (auto const& arg: _argv) {
        if (arg.substr(0,2) == "--") {
            std::string const nameEqualValue = arg.substr(2);
            if (nameEqualValue.empty()) {
                std::cerr
                    << "CmdParser::parse: illegal command line argument: " << arg << "\n"
                    << _usage << std::endl;
                throw std::invalid_argument(
                    "CmdParser::parse: illegal command line argument: " + arg);
            }
            std::string::size_type const equalPos = nameEqualValue.find("=");
            if (equalPos == std::string::npos) {
                if (nameEqualValue == "help") {
                    std::cerr
                        << _usage << std::endl;
                    throw std::invalid_argument (
                        "CmdParser::parse: help mode intercepted");
                }
                _flag.insert(nameEqualValue);
            } else {
                std::string const option = nameEqualValue.substr(0, equalPos);
                std::string const value  = nameEqualValue.substr(equalPos+1);
                if (value.empty()) {
                    std::cerr
                        << "CmdParser::parse: no value provided for option: " << option << "\n"
                        << _usage << std::endl;
                    throw std::invalid_argument (
                        "CmdParser::parse: no value provided for option: " + option);
                }
                _option[option] = value;
            }
        } else {
            _parameter.push_back(arg);
        }
    }
}

}}} // namespace lsst::qserv::replica