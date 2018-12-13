/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/ApplicationTypes.h"

// System headers
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace lsst {
namespace qserv {
namespace replica {
namespace detail {


ParserError::ParserError(util::Issue::Context const& ctx,
                         std::string const& message)
    :   util::Issue(ctx, "ParserError: " + message) {
}


std::ostream& operator<<(std::ostream& os, ArgumentParser const& arg) {
    arg.dumpNameValue(os);
    return os;
}


Command& Command::flag(std::string const& name,
                       std::string const& description,
                       bool& var) {
    _flags.emplace(
        std::make_pair(
            name,
            std::move(
                std::make_unique<FlagParser>(
                    name,
                    description,
                    var
                )
            )
        )
    );
    return *this;
}


CommandsSet::CommandsSet(std::vector<std::string> const& commandNames,
                         std::string& var)
    :   _var(var) {

    for (auto&& name: commandNames) {
        _commands.insert(std::make_pair(name, new Command()));
    }
}


Command& CommandsSet::command(std::string const& name) {
    auto itr = _commands.find(name);
    if (itr == _commands.end()) {
        throw std::range_error("CommandsSet::command  unknown command name: '" + name + "'");
    }
    return *(itr->second);
}


Parser::Parser(int argc,
               const char* const argv[],
               std::string const& description)
    :   _argc       (argc),
        _argv       (argv),
        _description(description),
        _code       (Status::UNDEFINED) {
}


void Parser::reset() {

    _required.clear();
    _optional.clear();
    _options.clear();
    _flags.clear();

    _code  = Status::UNDEFINED;
    _usage = "";
    _help  = "";
}


Parser& Parser::commands(std::string const& name,
                         std::vector<std::string> const& commandNames,
                         std::string& var) {

    if (_commands != nullptr) {
        throw std::logic_error("Parser::commands  the parser is already configured in this way");
    }
    verifyArgument(name);
    _commands = std::make_unique<CommandsSet>(commandNames, var);    
    return *this;
}


Command& Parser::command(std::string const& name) {
    if (_commands == nullptr) {
        throw std::logic_error("Parser::command  the parser is not configured in this way");
    }
    return _commands->command(name);
}


Parser& Parser::flag(std::string const& name,
                     std::string const& description,
                     bool& var) {
    verifyArgument(name);
    _flags.emplace(
        std::make_pair(
            name,
            std::move(
                std::make_unique<FlagParser>(
                    name,
                    description,
                    var
                )
            )
        )
    );
    return *this;
}


int Parser::parse() {

    // Check if the parser hasn't been used.
    if (Status::UNDEFINED != _code) return _code;

    // Intercept and respond to '--help' if found before parsing
    // any other arguments.
    for (int i = 1; i < _argc; ++i) {

        std::string const arg = _argv[i];

        if (arg == "--help") {
            std::cerr << help() << std::endl;
            return Status::HELP_REQUESTED;
        }
    }

    try {

        // Split input arguments into the following 3 categories, assuming
        // the following syntax:
        //
        //   options:   --<option>=[<value>]
        //   flag:      --<flag>
        //   parameter: <value>

        std::map<std::string, std::string> options;    
        std::set<std::string> flags;
        std::vector<std::string> parameters;

        if (_commands != nullptr) _commands->_var = "";

        for (int i = 1; i < _argc; ++i) {

            std::string const arg = _argv[i];

            // Positional parameter (or a command name)?
            if ("--" != arg.substr(0, 2)) {

                // Analyze and store  value of the very first positional parameter if
                // the Parser was configured for the 'commands' mode.
                if (_commands != nullptr and _commands->_var.empty()) {
                    if (_commands->_commands.end() == _commands->_commands.find(arg)) {
                        throw ParserError(ERR_LOC, "unknown command name: '" + arg + "'");
                    }
                    _commands->_var = arg;
                } else {
                    parameters.push_back(arg);
                }
                continue;
            }

            // An option with a value?
            std::string const nameVal = arg.substr(2);
            if (nameVal.empty()) {
                throw ParserError(ERR_LOC, "standalone '--' can't be used as a flag");
            }
            std::string::size_type const pos = nameVal.find('=');
            if (std::string::npos != pos) {
                std::string const name = nameVal.substr(0, pos);
                options[name] = nameVal.substr(pos+1);
                continue;
            }

            // Then, it's a flag
            flags.insert(nameVal);
        }
        if (_commands != nullptr and _commands->_var.empty()) {
            throw ParserError(ERR_LOC, "the command name is missing");
        }
 
        // Parse values of options
        for (auto&& entry: options) {

            auto const& name  = entry.first;
            auto const& value = entry.second;

            if (0 != _options.count(name)) {
                _options[name]->parse(value);
                continue;
            }
            if (_commands != nullptr) {
                auto& commandOptions = _commands->_commands[_commands->_var]->_options;
                if (0 != commandOptions.count(name)) {
                    commandOptions[name]->parse(value);
                    continue;
                }
            }
            throw ParserError(ERR_LOC, "'" + name+ "' is not an option");
        }
        
        // Parse flags
        for (auto&& name: flags) {
            if (0 != _flags.count(name)) {
                _flags[name]->parse();
            }
            if (_commands != nullptr) {
                auto& commandFlags = _commands->_commands[_commands->_var]->_flags;
                if (0 != commandFlags.count(name)) {
                    commandFlags[name]->parse();
                    continue;
                }
            }
            throw ParserError(ERR_LOC, "'" + name+ "' is not a flag");
        }

        // Verify that the number and a category (mandatory and optional)
        // of the positional parameters match expectations

        if (_commands != nullptr) {
            if (parameters.size() > _required.size() +
                                    _commands->_commands[_commands->_var]->_required.size() +
                                    _optional.size() +
                                    _commands->_commands[_commands->_var]->_optional.size()) {

                throw ParserError(ERR_LOC, "too many positional parameters");
            }
            if (parameters.size() < _required.size() +
                                    _commands->_commands[_commands->_var]->_required.size()) {

                throw ParserError(ERR_LOC, "insufficient number of positional parameters");
            }
        } else {
            if (parameters.size() > _required.size() + _optional.size()) {
                throw ParserError(ERR_LOC, "too many positional parameters");
            }
            if (parameters.size() < _required.size()) {
                throw ParserError(ERR_LOC, "insufficient number of positional parameters");
            }
        }

        // Then parse values of parameters

        if (_commands != nullptr) {

            size_t inIdxBegin = 0,
                   inIdxEnd   = 0;

            inIdxBegin  = inIdxEnd,
            inIdxEnd   += _required.size();

            for (size_t inIdx = inIdxBegin, outIdx = 0;
                        inIdx < inIdxEnd;
                      ++inIdx, ++outIdx) {
                _required[outIdx]->parse(parameters[inIdx]);
            }

            inIdxBegin  = inIdxEnd;
            inIdxEnd   += _commands->_commands[_commands->_var]->_required.size();

            for (size_t inIdx = inIdxBegin, outIdx = 0;
                        inIdx < inIdxEnd;
                      ++inIdx, ++outIdx) {
                _commands->_commands[_commands->_var]->_required[outIdx]->parse(parameters[inIdx]);
            }

            inIdxBegin = inIdxEnd;
            inIdxEnd   = std::min(inIdxEnd + _optional.size(),
                                  parameters.size());

            for (size_t inIdx = inIdxBegin, outIdx = 0;
                        inIdx < inIdxEnd;
                      ++inIdx, ++outIdx) {
                _optional[outIdx]->parse(parameters[inIdx]);
            }

            inIdxBegin = inIdxEnd;
            inIdxEnd   = std::min(inIdxEnd + _commands->_commands[_commands->_var]->_optional.size(),
                                  parameters.size());

            for (size_t inIdx = inIdxBegin, outIdx = 0;
                        inIdx < inIdxEnd;
                      ++inIdx, ++outIdx) {
                _commands->_commands[_commands->_var]->_optional[outIdx]->parse(parameters[inIdx]);
            }

        } else {
            for (size_t inIdx = 0,
                       outIdx = 0;
                 inIdx < _required.size();
                 ++inIdx,
                 ++outIdx) {

                _required[outIdx]->parse(parameters[inIdx]);
            }
            for (size_t  inIdx = _required.size(),
                        outIdx = 0;
                 inIdx < parameters.size();
                 ++inIdx,
                 ++outIdx) {

                _optional[outIdx]->parse(parameters[inIdx]);
            }
        }
        _code = Status::SUCCESS;

    } catch (ParserError const& ex) {
        std::cerr << ex.what() << "\n" << usage() << std::endl;
        _code = Status::PARSING_FAILED;
    }
    return _code;
}


void Parser::verifyArgument(std::string const& name) {

    if (name.empty()) {
        throw std::invalid_argument(
                "empty string passed where argument name was expected");
    }
    if (name == "help") {
        throw std::invalid_argument(
                "`help` is a reserved keyword");
    }
}


std::string const& Parser::usage() {

    std::string const indent = "  ";

    if (_usage.empty()) {
        _usage = "USAGE:\n";
        _usage += "\n" + indent + "--help\n";

        if (_commands == nullptr) {
            if (not (_required.empty() and _optional.empty())) {
                _usage += "\n" + indent;
                for (auto&& arg: _required) _usage += "<" + arg->name() + "> ";
                for (auto&& arg: _optional) _usage += "[<" + arg->name() + ">] ";
            }
            for (auto&& arg: _options)      _usage += "\n" + indent + "--" + arg.first + "=[<value>]";
            for (auto&& arg: _flags)        _usage += "\n" + indent + "--" + arg.first;
            _usage += "\n";
        } else {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                _usage += "\n" + indent + name + "  ";

                for (auto&& arg: _required)          _usage += "<"  + arg->name() + "> ";
                for (auto&& arg: command->_required) _usage += "<"  + arg->name() + "> ";
                for (auto&& arg: _optional)          _usage += "[<" + arg->name() + ">] ";
                for (auto&& arg: command->_optional) _usage += "[<" + arg->name() + ">] ";
                for (auto&& arg: _options)           _usage += "\n" + indent + "--" + arg.first + "=[<value>]";
                for (auto&& arg: command->_options)  _usage += "\n" + indent + "--" + arg.first + "=[<value>]";
                for (auto&& arg: _flags)             _usage += "\n" + indent + "--" + arg.first;
                for (auto&& arg: command->_flags)    _usage += "\n" + indent + "--" + arg.first;
                _usage += "\n";
            }
        }
    }
    return _usage;
}


std::string const& Parser::help() {

    if (_help.empty()) {

        _help += "DESCRIPTION:\n\n" + wrap(_description, "  ") + "\n\n" + usage();

        _help += "\nPARAMETERS:\n";

        for (auto&& arg: _required) {
            _help += "\n  <" + arg->name() + ">\n" + wrap(arg->description()) + "\n";
        }
        if (_commands != nullptr) {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                for (auto&& arg: command->_required) {
                    _help += "\n  <" + arg->name() + ">  [ " + name + " ]\n" + wrap(arg->description()) + "\n";
                }
            }
        }
        for (auto&& arg: _optional) {
            _help += "\n  <" + arg->name() + ">\n" + wrap(arg->description()) + "\n";
            _help += "\n        DEFAULT: " + arg->defaultValue() + "\n";
        }
        if (_commands != nullptr) {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                for (auto&& arg: command->_optional) {
                    _help += "\n  <" + arg->name() + ">  [ " + name + " ]\n" + wrap(arg->description()) + "\n";
                    _help += "\n        DEFAULT: " + arg->defaultValue() + "\n";
                }
            }
        }

        _help += "\nOPTIONS:\n";

        for (auto&& entry: _options) {
            auto&& arg = entry.second;
            _help += "\n  --" + arg->name() + "\n" + wrap(arg->description()) + "\n";
            _help += "\n        DEFAULT: " + arg->defaultValue() + "\n";
        }
        if (_commands != nullptr) {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                for (auto&& entry1: command->_options) {
                    auto&& arg = entry1.second;
                    _help += "\n  --" + arg->name() + "  [ " + name + " ]\n" + wrap(arg->description()) + "\n";
                    _help += "\n        DEFAULT: " + arg->defaultValue() + "\n";
                }
            }
        }

        _help += "\nFLAGS:\n";
        _help += "\n  --help\n" + wrap("print this 'help'") + "\n";

        for (auto&& entry: _flags) {
            auto&& arg = entry.second;
            _help += "\n  --" + arg->name() + "\n" + wrap(arg->description()) + "\n";
        }
        if (_commands != nullptr) {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                for (auto&& entry1: command->_flags) {
                    auto&& arg = entry1.second;
                    _help += "\n  --" + arg->name() + "  [ " + name + " ]\n" + wrap(arg->description()) + "\n";
                }
            }
        }
    }
    return _help;
}

std::string Parser::wrap(std::string const& str,
                         std::string const& indent,
                         size_t width) {
    
    std::ostringstream os;
    size_t lineLength = 0;

    std::istringstream is(str);
    std::string word;

    while (is >> word) {

        if (0 == lineLength) {
            
            // Just starting the very first line.

            os << indent;
            lineLength = indent.size();

        } else if (lineLength +  word.size() + 1 > width) {

            // Wrap the current line if its total length would exceed
            // the allowed limit after adding the word.

            os << "\n" << indent;
            lineLength = indent.size();
        }
        os << word << " ";
        lineLength += word.size() + 1;
    }
    return os.str();
}


std::string Parser::serializeArguments() const {

    if (Status::SUCCESS != _code) {
        throw std::logic_error(
            "Application::Parser::serializeArguments()"
            "  command line arguments have not been parsed yet");
    }
    std::ostringstream os;
    for (auto&& arg: _required) os << *arg << " ";
    for (auto&& arg: _optional) os << *arg << " ";
    for (auto&& arg: _options)  os << *arg.second << " ";
    for (auto&& arg: _flags)    os << *arg.second << " ";
    return os.str();
}

}}}} // namespace lsst::qserv::replica::detail
