/*
 * LSST Data Management System
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

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {
namespace detail {


ParserError::ParserError(util::Issue::Context const& ctx,
                         string const& message)
    :   util::Issue(ctx, string(__func__) + ": " + message) {
}


ostream& operator<<(ostream& os, ArgumentParser const& arg) {
    arg.dumpNameValue(os);
    return os;
}


Command& Command::flag(string const& name,
                       string const& description,
                       bool& var) {
    bool const reverse = false;
    _flags.emplace(
        name,
        make_unique<FlagParser>(
            name,
            description,
            var,
            reverse
        )
    );
    return *this;
}


Command& Command::reversedFlag(string const& name,
                               string const& description,
                               bool& var) {
    bool const reverse = true;
    _flags.emplace(
        name,
        make_unique<FlagParser>(
            name,
            description,
            var,
            reverse
        )
    );
    return *this;
}


CommandsSet::CommandsSet(vector<string> const& commandNames,
                         string& var)
    :   _var(var) {

    for (auto&& name: commandNames) {
        _commands.insert(make_pair(name, new Command()));
    }
}


Command& CommandsSet::command(string const& name) {
    auto itr = _commands.find(name);
    if (itr == _commands.end()) {
        throw range_error(
                "CommandsSet::" + string(__func__) + "  unknown command name: '" + name + "'");
    }
    return *(itr->second);
}


Parser::Parser(int argc,
               const char* const argv[],
               string const& description)
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

    _code     = Status::UNDEFINED;
    _usageStr = "";
    _helpStr  = "";
}


Parser& Parser::commands(string const& name,
                         vector<string> const& commandNames,
                         string& var) {

    if (_commands != nullptr) {
        throw logic_error("Parser::" + string(__func__) + "  the parser is already configured in this way");
    }
    _verifyArgument(name);
    _commands = make_unique<CommandsSet>(commandNames, var);    
    return *this;
}


Command& Parser::command(string const& name) {
    if (_commands == nullptr) {
        throw logic_error(
                "Parser::" + string(__func__) + "  the parser is not configured in this way");
    }
    return _commands->command(name);
}


Parser& Parser::flag(string const& name,
                     string const& description,
                     bool& var) {
    _verifyArgument(name);
    bool const reverse = false;
    _flags.emplace(
        make_pair(
            name,
            make_unique<FlagParser>(
                name,
                description,
                var,
                reverse
            )
        )
    );
    return *this;
}


Parser& Parser::reversedFlag(string const& name,
                             string const& description,
                             bool& var) {
    _verifyArgument(name);
    bool const reverse = true;
    _flags.emplace(
        make_pair(
            name,
            make_unique<FlagParser>(
                name,
                description,
                var,
                reverse
            )
        )
    );
    return *this;
}


Parser::Status Parser::parse() {

    // Check if the parser hasn't been used.
    if (Status::UNDEFINED != _code) return _code;

    // Intercept and respond to '--help' if found before parsing
    // any other arguments.
    for (int i = 1; i < _argc; ++i) {

        string const arg = _argv[i];

        if (arg == "--help") {
            cerr << _help() << endl;
            _code = Status::HELP_REQUESTED;
            return _code;
        }
    }

    bool const commandMode = (_commands != nullptr);

    try {

        // Split input arguments into the following 3 categories, assuming
        // the following syntax:
        //
        //   options:   --<option>=[<value>]
        //   flag:      --<flag>
        //   parameter: <value>

        map<string, string> inOptions;    
        set<string>         inFlags;
        vector<string>      inParameters;

        if (commandMode) _commands->_var = "";

        for (int i = 1; i < _argc; ++i) {

            string const arg = _argv[i];

            // Positional parameter (or a command name)?
            if ("--" != arg.substr(0, 2)) {

                // Analyze and store  value of the very first positional parameter if
                // the Parser was configured for the 'commands' mode.
                if (commandMode and _commands->_var.empty()) {
                    if (_commands->_commands.end() == _commands->_commands.find(arg)) {
                        throw ParserError(ERR_LOC, "unknown command name: '" + arg + "'");
                    }
                    _commands->_var = arg;
                } else {
                    inParameters.push_back(arg);
                }
                continue;
            }

            // An option with a value?
            string const nameVal = arg.substr(2);
            if (nameVal.empty()) {
                throw ParserError(ERR_LOC, "standalone '--' can't be used as a flag");
            }
            string::size_type const pos = nameVal.find('=');
            if (string::npos != pos) {
                string const name = nameVal.substr(0, pos);
                inOptions[name] = nameVal.substr(pos+1);
                continue;
            }

            // Then, it's a flag
            inFlags.insert(nameVal);
        }
        if (commandMode and _commands->_var.empty()) {
            throw ParserError(ERR_LOC, "the command name is missing");
        }
        string const commandName = commandMode ? _commands->_var : string();
 
        // Parse values of options
        for (auto&& entry: inOptions) {

            auto const& name  = entry.first;
            auto const& value = entry.second;

            if (_parseOption(_options, name, value)) continue;
            if (commandMode) {
                if (_parseOption(_commands->_commands[commandName]->_options, name, value)) continue;
            }
            throw ParserError(ERR_LOC, "'" + name+ "' is not an option");
        }
        
        // Parse flags
        for (auto&& name: inFlags) {
            if (_parseFlag(_flags, name)) continue;
            if (commandMode) {
                if (_parseFlag(_commands->_commands[commandName]->_flags, name)) continue;
            }
            throw ParserError(ERR_LOC, "'" + name+ "' is not a flag");
        }

        // Verify that the number and a category (mandatory and optional)
        // of the positional inParameters match expectations

        size_t const inNumParameters = inParameters.size();

        size_t const maxNumParameters =
            _required.size() +
            _optional.size() + (commandMode ? 
            _commands->_commands[commandName]->_required.size() +
            _commands->_commands[commandName]->_optional.size() : 0);

        if (inNumParameters > maxNumParameters) {
            throw ParserError(
                    ERR_LOC,
                    "too many positional parameters " + to_string(inNumParameters) +
                    ", expected no more than " + to_string(maxNumParameters));
        }

        size_t const minNumParameters =
            _required.size() + (commandMode ?
             _commands->_commands[commandName]->_required.size() : 0);
 
        if (inNumParameters < minNumParameters) {
            throw ParserError(
                    ERR_LOC,
                    "insufficient number " + to_string(inNumParameters) +
                    " of positional parameters, expected at least " + to_string(minNumParameters));
        }

        // Then parse values of parameters
        
        auto       inItr    = inParameters.cbegin();
        auto const inItrEnd = inParameters.cend();

        if (commandMode) {
            _parseParameters(_required,                                    inItr, inItrEnd);
            _parseParameters(_commands->_commands[commandName]->_required, inItr, inItrEnd);
            _parseParameters(_optional,                                    inItr, inItrEnd);
            _parseParameters(_commands->_commands[commandName]->_optional, inItr, inItrEnd);
        } else {
            _parseParameters(_required,                                    inItr, inItrEnd);
            _parseParameters(_optional,                                    inItr, inItrEnd);
        }
        _code = Status::SUCCESS;

    } catch (ParserError const& ex) {
        cerr << ex.what() << "\n" << _usage() << endl;
        _code = Status::PARSING_FAILED;
    }
    return _code;
}


bool Parser::_parseOption(map<string, unique_ptr<ArgumentParser>>& options,
                          string const& name,
                          string const& value) {
    auto itr = options.find(name);
    if (itr == options.end()) return false;
    (*itr).second->parse(value);
    return true;
}


bool Parser::_parseFlag(map<string, unique_ptr<ArgumentParser>>& flags,
                        string const& name) {
    auto itr = flags.find(name);
    if (itr == flags.end()) return false;
    (*itr).second->parse();
    return true;   
}


void Parser::_parseParameters(vector<unique_ptr<ArgumentParser>>& out,
                              vector<string>::const_iterator& inItr,
                              vector<string>::const_iterator const& inItrEnd) {

    auto outItr = out.cbegin();
    while (outItr != out.cend() and inItr != inItrEnd) {
        (*(outItr++))->parse(*(inItr++));
    }

}


void Parser::_verifyArgument(string const& name) {

    if (name.empty()) {
        throw invalid_argument(
                "Parser::" + string(__func__) + "  empty string passed where argument name "
                "was expected");
    }
    if (name == "help") {
        throw invalid_argument("Parser::" + string(__func__) + " `help` is a reserved keyword");
    }
}


string const& Parser::_usage() {

    string const indent = "  ";

    if (_usageStr.empty()) {
        _usageStr = "USAGE:\n";
        _usageStr += "\n" + indent + "--help\n";

        if (_commands == nullptr) {
            if (not (_required.empty() and _optional.empty())) {
                _usageStr += "\n" + indent;
                for (auto&& arg: _required) _usageStr += "<" + arg->name() + "> ";
                for (auto&& arg: _optional) _usageStr += "[<" + arg->name() + ">] ";
            }
            for (auto&& arg: _options)      _usageStr += "\n" + indent + "--" + arg.first + "=[<value>]";
            for (auto&& arg: _flags)        _usageStr += "\n" + indent + "--" + arg.first;
            _usageStr += "\n";
        } else {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                _usageStr += "\n" + indent + name + "  ";

                for (auto&& arg: _required)          _usageStr += "<"  + arg->name() + "> ";
                for (auto&& arg: command->_required) _usageStr += "<"  + arg->name() + "> ";
                for (auto&& arg: _optional)          _usageStr += "[<" + arg->name() + ">] ";
                for (auto&& arg: command->_optional) _usageStr += "[<" + arg->name() + ">] ";
                for (auto&& arg: _options)           _usageStr += "\n" + indent + "--" + arg.first + "=[<value>]";
                for (auto&& arg: command->_options)  _usageStr += "\n" + indent + "--" + arg.first + "=[<value>]";
                for (auto&& arg: _flags)             _usageStr += "\n" + indent + "--" + arg.first;
                for (auto&& arg: command->_flags)    _usageStr += "\n" + indent + "--" + arg.first;
                _usageStr += "\n";
            }
        }
    }
    return _usageStr;
}


string const& Parser::_help() {

    if (_helpStr.empty()) {

        bool const commandMode = (_commands != nullptr);

        _helpStr += "DESCRIPTION:\n\n" + _wrap(_description, "  ") + "\n\n" + _usage();

        if (commandMode) {
            _helpStr += "\nCOMMANDS:\n";
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                _helpStr += "\n  " + name + "\n" + _wrap(command->_description) + "\n";
            }
        }
        _helpStr += "\nPARAMETERS:\n";
        for (auto&& arg: _required) {
            _helpStr += "\n  <" + arg->name() + ">\n" + _wrap(arg->description()) + "\n";
        }
        if (commandMode) {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                for (auto&& arg: command->_required) {
                    _helpStr += "\n  <" + arg->name() + ">  [ " + name + " ]\n" + _wrap(arg->description()) + "\n";
                }
            }
        }
        for (auto&& arg: _optional) {
            _helpStr += "\n  <" + arg->name() + ">\n" + _wrap(arg->description()) + "\n";
            _helpStr += "\n        DEFAULT: " + arg->defaultValue() + "\n";
        }
        if (commandMode) {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                for (auto&& arg: command->_optional) {
                    _helpStr += "\n  <" + arg->name() + ">  [ " + name + " ]\n" + _wrap(arg->description()) + "\n";
                    _helpStr += "\n        DEFAULT: " + arg->defaultValue() + "\n";
                }
            }
        }

        _helpStr += "\nOPTIONS:\n";

        for (auto&& entry: _options) {
            auto&& arg = entry.second;
            _helpStr += "\n  --" + arg->name() + "\n" + _wrap(arg->description()) + "\n";
            _helpStr += "\n        DEFAULT: " + arg->defaultValue() + "\n";
        }
        if (commandMode) {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                for (auto&& entry1: command->_options) {
                    auto&& arg = entry1.second;
                    _helpStr += "\n  --" + arg->name() + "  [ " + name + " ]\n" + _wrap(arg->description()) + "\n";
                    _helpStr += "\n        DEFAULT: " + arg->defaultValue() + "\n";
                }
            }
        }

        _helpStr += "\nFLAGS:\n";
        _helpStr += "\n  --help\n" + _wrap("print this 'help'") + "\n";

        for (auto&& entry: _flags) {
            auto&& arg = entry.second;
            _helpStr += "\n  --" + arg->name() + "\n" + _wrap(arg->description()) + "\n";
        }
        if (commandMode) {
            for (auto const& entry: _commands->_commands) {

                auto const& name    = entry.first;
                auto const& command = entry.second;

                for (auto&& entry1: command->_flags) {
                    auto&& arg = entry1.second;
                    _helpStr += "\n  --" + arg->name() + "  [ " + name + " ]\n" + _wrap(arg->description()) + "\n";
                }
            }
        }
    }
    return _helpStr;
}


string Parser::_wrap(string const& str,
                     string const& indent,
                     size_t width) {
    
    ostringstream os;
    size_t lineLength = 0;

    istringstream is(str);
    string word;

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


string Parser::serializeArguments() const {

    if (Status::SUCCESS != _code) {
        throw logic_error(
                "Parser::" + string(__func__) + "  command line arguments have not been parsed yet");
    }
    ostringstream os;
    for (auto&& arg: _required) os << *arg << " ";
    for (auto&& arg: _optional) os << *arg << " ";
    for (auto&& arg: _options)  os << *arg.second << " ";
    for (auto&& arg: _flags)    os << *arg.second << " ";
    return os.str();
}

}}}} // namespace lsst::qserv::replica::detail
