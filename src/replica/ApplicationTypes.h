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
#ifndef LSST_QSERV_REPLICA_APPLICATION_TYPES_H
#define LSST_QSERV_REPLICA_APPLICATION_TYPES_H

/**
 * ApplicationTypes.h declares types which are used in an implementation
 * of class Application. These types are put into this header to avoid
 * cluttered the host's class header with too many details.
 */

// System headers
#include <map>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// Third party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "replica/Common.h"
#include "util/Issue.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace detail {
    class Parser;
}}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace detail {

/**
 * Class ParserError represents exceptions throw by the command-line parser
 * during processing arguments as per user requested syntax description.
 */
class ParserError : public util::Issue {
public:
    ParserError(util::Issue::Context const& ctx,
                std::string const& message);
};


/**
 * The very base class which represents subjects which are parsed:
 * parameters, options and flags.
 */
class ArgumentParser {

public:

    /**
     * @return
     *   'true' if the specified value belongs to a collection
     *
     * @param val
     *   the value to be evaluated
     *
     * @param col
     *   the collection of allowed values
     */
    template <typename T>
    static bool in(T const& val,
                  std::vector<T> const& col) {
        return col.end() != std::find(col.begin(), col.end(), val);
    }

     // Default construction and copy semantics are prohibited

    ArgumentParser() = delete;
    ArgumentParser(ArgumentParser const&) = delete;
    ArgumentParser& operator=(ArgumentParser const&) = delete;

    /**
     * Construct the object
     *
     * @param name
     *   the name of the parameter as it will be shown in error messages
     *   (should there be any problem with parsing a value of the parameter)
     *   and the 'help' printout (if the one is requested in the constructor
     *   of the class)
     *
     * @param description
     *   the description of the parameter as it will be shown in the 'help'
     *   printout (if the one is requested in the constructor of the class)
     */
    ArgumentParser(std::string const& name,
                   std::string const& description)
    :   _name(name),
        _description(description) {
    }

    virtual ~ArgumentParser() = default;

    // Trivial state retrieval methods

    std::string const& name() const { return _name; }
    std::string const& description() const { return _description; }

    /**
     * Let a subclass to parse the input string into a value of the corresponding
     * (as defined by the class) type.
     *
     * @param inStr
     *   (optional) input string to be parsed
     *
     * @throws ParserErro
     *   if the text can't be parsed
     */
    virtual void parse(std::string const& inStr="") = 0;

    /**
     * Default values are supposed to be captured from user-defined variables
     * at a time when the argument objects are constructed. They're used for
     * generating a documentation.
     *
     * @return
     *   string representation of the default value of an argument
     */
    virtual std::string defaultValue() const = 0;

    /**
     * Dump the name of an argument and its value into an output stream
     *
     * @param
     *   an output stream object
     */
    virtual void dumpNameValue(std::ostream& os) const = 0;

private:
    
    // Parameters of the object passed via the class's constructor

    std::string const _name;
    std::string const _description;
};


/**
 * Dump a string representation of the argument name and its value
 * to the stream.
 */
std::ostream& operator<<(std::ostream& os, ArgumentParser const& arg);


/**
 * The class representing (mandatory or optional) parameters
 */
template <typename T>
class ParameterParser : public ArgumentParser {

public:

     // Default construction and copy semantics are prohibited

    ParameterParser() = delete;
    ParameterParser(ParameterParser const&) = delete;
    ParameterParser& operator=(ParameterParser const&) = delete;

    /**
     * Construct the object
     *
     * @param name
     *   the name of the parameter as it will be shown in error messages
     *   (should there be any problem with parsing a value of the parameter)
     *   and the 'help' printout (if the one is requested in the constructor
     *   of the class)
     *
     * @param description
     *   the description of the parameter as it will be shown in the 'help'
     *   printout (if the one is requested in the constructor of the class)
     *
     * @param var
     *   the reference to the corresponding variable to be initialized with
     *   a value of the parameter after successful parsing. The type of the
     *   parameter is determined by the template argument.
     *
     * @see ArgumentParser::ArgumentParser()
     */
    ParameterParser(std::string const& name,
                    std::string const& description,
                    T& var,
                    std::vector<T> const& allowedValues)
    :   ArgumentParser(name,
                       description),
        _var(var),
        _defaultValue(var),
        _allowedValues(allowedValues) {
    }

    virtual ~ParameterParser() = default;

    /// @see ArgumentParser::parse()
    void parse(std::string const& inStr="") final {
        try {
            _var = boost::lexical_cast<T>(inStr);
        } catch (boost::bad_lexical_cast const& ex) {
            throw ParserError(
                    ERR_LOC,
                    "failed to parse a value of parameter '" + name() +
                    " from '" + inStr);
        }
        if (not _allowedValues.empty()) {
            if (not in(_var, _allowedValues)) {
                throw ParserError(
                    ERR_LOC,
                    "the value of parameter '" + name() + "' is disallowed: '" + inStr + "'");
            }
        }
    }

    /// @see ArgumentParser::defaultValue()
    std::string defaultValue() const final {
        std::ostringstream os;
        os << _defaultValue;
        return os.str();
    }

    /// @see ArgumentParser::dumpNameValue()
    void dumpNameValue(std::ostream& os) const final {
        os << name() << "=" << _var;
    }

private:
    
    /// A reference to a user variable to be initialized
    T& _var;
    
    /// A copy of the variable is captured here
    T const _defaultValue;

    /// A collection of allowed values (if provided)
    std::vector<T> const _allowedValues;
};


/**
 * The class representing named options
 */
template <typename T>
class OptionParser : public ArgumentParser {

public:

     // Default construction and copy semantics are prohibited

    OptionParser() = delete;
    OptionParser(OptionParser const&) = delete;
    OptionParser& operator=(OptionParser const&) = delete;

    /**
     * Construct the object
     *
     * @param name
     *   the name of the parameter as it will be shown in error messages
     *   (should there be any problem with parsing a value of the parameter)
     *   and the 'help' printout (if the one is requested in the constructor
     *   of the class)
     *
     * @param description
     *   the description of the parameter as it will be shown in the 'help'
     *   printout (if the one is requested in the constructor of the class)
     *
     * @param var
     *   the reference to the corresponding variable to be initialized with
     *   a value of the parameter after successful parsing. The type of the
     *   parameter is determined by the template argument.
     *
     * @see ArgumentParser::ArgumentParser()
     */
    OptionParser(std::string const& name,
                 std::string const& description,
                 T& var)
    :   ArgumentParser(name,
                       description),
        _var(var),
        _defaultValue(var) {
    }

    virtual ~OptionParser() = default;

    /// @see ArgumentParser::parse()
    void parse(std::string const& inStr="") final {
        if (inStr.empty()) return;
        try {
            _var = boost::lexical_cast<T>(inStr);
        } catch (boost::bad_lexical_cast const& ex) {
            throw ParserError(ERR_LOC,
                              "failed to parse a value of option '" + name() + " from '" + inStr);
        }
    }

    /// @see ArgumentParser::defaultValue()
    std::string defaultValue() const final {
        std::ostringstream os;
        os << _defaultValue;
        return os.str();
    }

    /// @see ArgumentParser::dumpNameValue()
    void dumpNameValue(std::ostream& os) const final {
        os << name() << "=" << _var;
    }

private:
    
    /// A reference to a user variable to be initialized
    T& _var;

    /// A copy of the variable is captured here
    T const _defaultValue;
};


/**
 * The class representing named flags
 */
class FlagParser : public ArgumentParser {

public:

     // Default construction and copy semantics are prohibited

    FlagParser() = delete;
    FlagParser(FlagParser const&) = delete;
    FlagParser& operator=(FlagParser const&) = delete;

    /**
     * Construct the object
     * 
     * @param name
     *   the name of the parameter as it will be shown in error messages
     *   (should there be any problem with parsing a value of the parameter)
     *   and the 'help' printout (if the one is requested in the constructor
     *   of the class)
     *
     * @param description
     *   the description of the parameter as it will be shown in the 'help'
     *   printout (if the one is requested in the constructor of the class)
     *
     * @param var
     *   the reference to the corresponding variable to be initialized with
     *   a value of the parameter after successful parsing. The type of the
     *   parameter is determined by the template argument.
     *
     * @param reverse
     *   the parameter which would reverse the behavior of the parser
     *   after finding the flag. If the parameter is set to 'true' then the result
     *   will be reset to 'false'. The default behavior of the parser is to set
     *   the result to 'true' if the flag was found.
     *
     * @see ArgumentParser::ArgumentParser()
     */
    FlagParser(std::string const& name,
               std::string const& description,
               bool& var,
               bool reverse)
    :   ArgumentParser(name,
                       description),
        _var(var),
        _reverse(reverse) {
    }

    virtual ~FlagParser() = default;

    /// @see ArgumentParser::parse()
    void parse(std::string const& inStr="") final { _var = _reverse ? false : true; }

    /// @see ArgumentParser::defaultValue()
    std::string defaultValue() const final { return _reverse ? "true" : "false"; }

    /// @see ArgumentParser::dumpNameValue()
    void dumpNameValue(std::ostream& os) const final {
        os << name() << "=" << bool2str(_var);
    }

private:
    
    /// A reference to a user variable to be initialized
    bool& _var;

    /// The flag value reversing option
    bool _reverse;
};


/**
 * Class Command is an abstraction for commands
 */
class Command {

public:

    // The default constructor
    Command() = default;

    // The copy semantics is prohibited

    Command(Command const&) = delete;
    Command& operator=(Command const&) = delete;

    ~Command() = default;

    /**
     * Set a description of the command
     *
     * @param description
     *   the description of the command as it will be shown in the 'help'
     *   printout (if the one is requested in the constructor of the class)
     *
     * @return
     *   a reference to the command object in order to allow chained calls
     */
    Command& description(std::string const& descr) {
        _description = descr;
        return *this;
    }

    /**
     * Register a mandatory positional parameter for parsing. Positional
     * parameters are lined up based on an order in which the positional
     * parameter methods (this and 'optional') are called.
     *
     * @see method Command::optional()
     *
     * @param name
     *   the name of the parameter as it will be shown in error messages
     *   (should there be any problem with parsing a value of the parameter)
     *   and the 'help' printout (if the one is requested in the constructor
     *   of the class)
     *
     * @param description
     *   the description of the parameter as it will be shown in the 'help'
     *   printout (if the one is requested in the constructor of the class)
     *
     * @param var
     *   the reference to the corresponding variable to be initialized with
     *   a value of the parameter after successful parsing. The type of the
     *   parameter is determined by the template argument.
     *
     * @param allowedValues
     *   (optional) collection of allowed values of the parameter.
     *
     * @throws std::invalid_argument
     *   if the name of the argument is empty, or if another parameter, option
     *   or flag under the same name was already requested earlier.
     *
     * @return
     *   a reference to the command object in order to allow chained calls
     */
    template <typename T>
    Command& required(std::string const& name,
                      std::string const& description,
                      T& var,
                      std::vector<T> const& allowedValues = std::vector<T>()) {
        _required.push_back(
            std::make_unique<ParameterParser<T>>(
                name,
                description,
                var,
                allowedValues
            )
        );
        return *this;
    }

    /**
     * Register an optional positional parameter for parsing. The original
     * state of a variable passed into the method will assumed as the default
     * value of the parameter. The value will stay intact if the parameter
     * won't be found in a command line. Otherwise this method is similar to
     * the above defined 'required'.
     *
     * @see method Command::required()
     *
     * @return
     *   a reference to the command object in order to allow chained calls
     */
    template <typename T>
    Command& optional(std::string const& name,
                      std::string const& description,
                      T& var,
                      std::vector<T> const& allowedValues = std::vector<T>()) {
        _optional.push_back(
            std::make_unique<ParameterParser<T>>(
                name,
                description,
                var,
                allowedValues
            )
        );
        return *this;
    }

    /**
     * Register a named option which has a value. The method is similar to
     * the above defined 'required' except it may
     * show up at any position in the command line.
     *
     * @see method Command::optional()
     *
     * @return
     *   a reference to the command object in order to allow chained calls
     */
    template <typename T>
    Command& option(std::string const& name,
                    std::string const& description,
                    T& var) {
        _options.emplace(
            name,
            std::make_unique<OptionParser<T>>(
                name,
                description,
                var
            )
        );
        return *this;
    }

    /**
     * Register a named flag. If the flag will be found among the command
     * line parameters then the variable will be set to 'true'. Otherwise
     * it will be set to 'false'. Other parameters of the method are similar
     * to the ones of the above defined 'add' methods.
     *
     * @see method Command::option()
     *
     * @return
     *   a reference to the command object in order to allow chained calls
     */
    Command& flag(std::string const& name,
                  std::string const& description,
                  bool& var);

    /**
     * This variation of the flag registration method would result in reversing
     * result if a flag s found in the command line.
     *
     * @see method Command::flag()
     */
    Command& reversedFlag(std::string const& name,
                          std::string const& description,
                          bool& var);

private:

    /// The friend class is allowed to access the members when parsing
    /// the command-line input
    friend class Parser;

    /// The optional description of the command
    std::string _description;

    /// A sequence of the mandatory parameters
    std::vector<std::unique_ptr<ArgumentParser>> _required;
    
    /// A sequence of the optional parameters
    std::vector<std::unique_ptr<ArgumentParser>> _optional;

    /// A set of named options
    std::map<std::string, std::unique_ptr<ArgumentParser>> _options;

    /// A set of named flags
    std::map<std::string, std::unique_ptr<ArgumentParser>> _flags;
};


/**
 * Class CommandsSet encapsulates a collection of commands along
 * with command-specific parameters.
 */
class CommandsSet {

public:

     // Default construction and copy semantics are prohibited

    CommandsSet() = delete;
    CommandsSet(CommandsSet const&) = delete;
    CommandsSet& operator=(CommandsSet const&) = delete;

    /**
     * Construct the object
     * 
     * @param commandNames
     *   a collection of command names
     *
     * @param var
     *   a user variable to be initialized with the name of a command detected
     *   by the Parser.
     */
    CommandsSet(std::vector<std::string> const& commandNames,
                std::string& var);

    ~CommandsSet() = default;

    /**
     * Find a command in the set
     *
     * @param name
     *   the name of a command
     *
     * @return
     *   a reference to the command description object
     *
     * @throws std::range_error
     *   if the command is unknown
     */
    Command& command(std::string const& name);

private:

    /// The friend class is allowed to access the members when parsing
    /// the command-line input
    friend class Parser;

    /// A collection of commands
    std::map<std::string, std::unique_ptr<Command>> _commands;

    /// A reference to a user variable to be initialized
    std::string& _var;
};


/**
 * The class for parsing command line parameters and filling variables
 * provided by a user.
 */
class Parser {

public:
    
    enum Status {

        /// The initial state for the completion code. It's used to determine
        /// if any parsing attempt has been made.
        UNDEFINED = -1,

        /// The normal completion status
        SUCCESS = 0,

        /// This status is reported after intercepting flag "--help" and printing
        /// the documentation.
        HELP_REQUESTED = 1,

        /// The status is used to report any problem with parsing arguments.
        PARSING_FAILED = 2
    };
    
     // Default construction and copy semantics are prohibited

    Parser() = delete;
    Parser(Parser const&) = delete;
    Parser& operator=(Parser const&) = delete;

    virtual ~Parser() = default;

    /**
     * Construct and initialize the parser
     *
     * @param arc
     *   argument count
     *
     * @parav argv
     *   vector of argument values
     *
     * @param description
     *   description of an application
     */
    Parser(int argc,
           const char* const argv[],
           std::string const& description);

    /**
     * Reset the state of the object to the one it was constructed. This
     * means that all effects of the below defined 'add' and 'parse' methods
     * will be eliminated.
     *
     * IMPORTANT: the operation will NOT return user variables mentioned in
     * the 'add' methods back to their states if method 'parse' has already
     * been called. It's up to a user to reset those variables back to the
     * desired state if their intent behind calling this ('reset') method is
     * to reconfigure the parser and start over.
     *
     * @see method Parser::reset()
     */
    void reset();

    /**
     * Configure the Parser as the parser of "commands".
     *
     * @note
     *   This method can be called just once. Any subsequent attempts to call
     *   the methods will result in throwing exception std::logic_error.
     *
     * @param name
     *   the name of the parameter as it will be shown in error messages
     *   (should there be any problem with parsing a value of the parameter)
     *   and the 'help' printout (if the one is requested in the constructor
     *   of the class)
     *
     * @param commandNames
     *   a collection of column names
     *
     * @param var
     *   a user variable to be initialized with the name of a command detected
     *   by the Parser.
     *
     * @return
     *   a reference to the parser object in order to allow chained calls
     * 
     * @throws std::logic_error
     *   if the Parser was already configured in this way
     */
    Parser& commands(std::string const& name,
                     std::vector<std::string> const& commandNames,
                     std::string& var);

    /**
     * Find a command in the set
     *
     * @param name
     *   the name of a command
     *
     * @return
     *   a reference to the command description object
     *
     * @throws std::logic_error
     *   if the Parser was not configured in this way
     *
     * @throws std::range_error
     *   if the command is unknown
     */
    Command& command(std::string const& name);

    /**
     * Register a mandatory positional parameter for parsing. Positional
     * parameters are lined up based on an order in which the positional
     * parameter methods (this and 'optional') are
     * a being called.
     *
     * @see method Parser::optional()
     *
     * @param name
     *   the name of the parameter as it will be shown in error messages
     *   (should there be any problem with parsing a value of the parameter)
     *   and the 'help' printout (if the one is requested in the constructor
     *   of the class)
     *
     * @param description
     *   the description of the parameter as it will be shown in the 'help'
     *   printout (if the one is requested in the constructor of the class)
     *
     * @param var
     *   the reference to the corresponding variable to be initialized with
     *   a value of the parameter after successful parsing. The type of the
     *   parameter is determined by the template argument.
     *
     * @param allowedValues
     *   (optional) collection of allowed values of the parameter.
     *
     * @throws std::invalid_argument
     *   if the name of the argument is empty, or if another parameter, option
     *   or flag under the same name was already requested earlier.
     *
     * @return
     *   a reference to the parser object in order to allow chained calls
     */
    template <typename T>
    Parser& required(std::string const& name,
                              std::string const& description,
                              T& var,
                              std::vector<T> const& allowedValues = std::vector<T>()) {
        _verifyArgument(name);
        _required.push_back(
            std::move(
                std::make_unique<ParameterParser<T>>(
                    name,
                    description,
                    var,
                    allowedValues
                )
            )
        );
        return *this;
    }

    /**
     * Register an optional positional parameter for parsing. The original
     * state of a variable passed into the method will assumed as the default
     * value of the parameter. The value will stay intact if the parameter
     * won't be found in a command line. Otherwise this method is similar to
     * the above defined 'required'.
     *
     * @see method Parser::required()
     *
     * @return
     *   a reference to the parser object in order to allow chained calls
     */
    template <typename T>
    Parser& optional(std::string const& name,
                              std::string const& description ,
                              T& var,
                              std::vector<T> const& allowedValues = std::vector<T>()) {
        _verifyArgument(name);
        _optional.push_back(
            std::move(
                std::make_unique<ParameterParser<T>>(
                    name,
                    description,
                    var,
                    allowedValues
                )
            )
        );
        return *this;
    }

    /**
     * Register a named option which has a value. The method is similar to
     * the above defined 'required' except it may
     * show up at any position in the command line.
     *
     * @see method Parser::optional()
     *
     * @return
     *   a reference to the parser object in order to allow chained calls
     */
    template <typename T>
    Parser& option(std::string const& name,
                   std::string const& description,
                   T& var) {
        _verifyArgument(name);
        _options.emplace(
            std::make_pair(
                name,
                std::move(
                    std::make_unique<OptionParser<T>>(
                        name,
                        description,
                        var
                    )
                )
            )
        );
        return *this;
    }

    /**
     * Register a named flag. If the flag will be found among the command
     * line parameters then the variable will be set to 'true'. Otherwise
     * it will be set to 'false'. Other parameters of the method are similar
     * to the ones of the above defined 'add' methods.
     *
     * @see method Parser::option()
     *
     * @return
     *   a reference to the parser object in order to allow chained calls
     */
    Parser& flag(std::string const& name,
                 std::string const& description,
                 bool& var);

    /**
     * This variation of the flag registration method would result in reversing
     * result if a flag s found in the command line.
     *
     * @see method Parser::flag()
     */
    Parser& reversedFlag(std::string const& name,
                         std::string const& description,
                         bool& var);

    /**
     * Parse parameters, options and flags requested by above
     * defined 'add' methods. The method will return one the following
     * codes defined by Status.
     *
     * IMPORTANT: after completion (successful or not) the states of
     * some (or all) variables mentioned in the above defined methods
     * will change. It's up to a user to reset them back if the parser
     * will get reset (using method 'reset') and reconfigured.
     *
     * @see Parser::Status
     * @see method Parser::reset()
     *
     * @return
     *   completion code
     *
     * @throws ParserError
     *   for any problems occurring during the parsing
     */
    int parse();

    /**
     * @return
     *   serialize names and values of the parsed arguments serialized
     *   into a string
     *
     * @throws std::logic_error
     *   if called before attempted to parse
     *   the command line parameters, or if the parsing didn't successfully
     *   finish with Status::SUCCESS.
     */
    std::string serializeArguments() const;

private:

    /**
     * Verify the name of an argument (parameter, option or flag) to ensure 
     * it has a valid name.
     *
     * @param name
     *   the name of an argument
     * 
     * @throws std::invalid_argument 
     *   if the name is not allowed or it's empty
     */
    void _verifyArgument(std::string const& name);

    /**
     * Parse and store a value of an option in a collection if it's a valid option
     * 
     * @param options
     *   a collection of options to be updated
     *
     * @param name
     *   the name of an option
     *
     * @param value
     *   its value
     *
     * @return
     *   'true' of this is a valid option, and it's been successfully parsed
     */
    bool _parseOption(std::map<std::string, std::unique_ptr<ArgumentParser>>& options,
                      std::string const& name,
                      std::string const& value);

    /**
     * Parse and store a value of a flag in a collection if it's a valid flag
     * 
     * @param options
     *   a collection of flags to be updated
     *
     * @param name
     *   the name of a flag
     *
     * @return
     *   'true' of this is a valid flag, and it's been successfully parsed
     */
    bool _parseFlag(std::map<std::string, std::unique_ptr<ArgumentParser>>& flags,
                    std::string const& name);

   /**
    * Parse and store values of the positional parameters
    * 
    * @param out
    *   the output collection of parameters to be populated from the input one
    *
    * @param inItr
    *   the current position of a modifiable iterator pointing to the input
    *   collection of parameters to be analyzed and parsed
    * 
    * @param inItrEnd
    *   the end iterator for the input collection of parameters to be analyzed
    *   and parsed
    */ 
    void _parseParameters(std::vector<std::unique_ptr<ArgumentParser>>& out,
                          std::vector<std::string>::const_iterator& inItr,
                          std::vector<std::string>::const_iterator const& inItrEnd);

    /**
     * @return
     *   the "Usage" string to be reported in case if any problem
     *   with parsing the command line arguments will be seen. The current
     *   implementation of this method will build and cache the string the
     *   first time the method is invoked.
     */
    std::string const& _usage();

    /**
     * @return
     *   the complete documentation to be returned if flag "--help"
     *   is passed as an argument.  The current implementation of this method
     *   will build and cache the string the first time the method is invoked
     *   regardless if flag "--help" is registered or not for the application.
     */
    std::string const& _help();
    
    /**
     * Read the input string and produce an output one with words
     * wrapped at wite spaces not to exceed the specified maximum width
     * of each line.
     *
     * @param str
     *   the input string to be wrapped
     *   
     * @param indent
     *   the indent at the beginning of each line
     *   
     * @param width
     *   the maximum width of each line (including the specified indent)
     *
     * @return
     *   the wrapped text, in which each line (including the last one)
     *   ends with the newline symbol.
     */
    static std::string _wrap(std::string const& str,
                             std::string const& indent="      ",
                             size_t width=72);


    // Input parameters

    int         const  _argc;
    const char* const* _argv;
    std::string const  _description;

    /// A sequence of the mandatory parameters
    std::vector<std::unique_ptr<ArgumentParser>> _required;
    
    /// A sequence of the optional parameters
    std::vector<std::unique_ptr<ArgumentParser>> _optional;

    /// A set of named options
    std::map<std::string, std::unique_ptr<ArgumentParser>> _options;

    /// A set of named flags
    std::map<std::string, std::unique_ptr<ArgumentParser>> _flags;

    /// A set of commands
    std::unique_ptr<CommandsSet> _commands;

    /// Status code set after parsing the arguments. It's also used to avoid
    /// invoking method Parser::parse() more than one time. The default value
    /// indicates that the parser has never attempted.
    int _code;

    /// The "Usage" string is build when all arguments are registered
    /// and method 'parse()' is invoked.
    std::string _usageStr;

    /// The documentation (invoked with "--help") string is build when all
    /// arguments are registered and method 'parse()' is invoked.
    std::string _helpStr;
};

}}}} // namespace lsst::qserv::replica::detail

#endif // LSST_QSERV_REPLICA_APPLICATION_TYPES_H

