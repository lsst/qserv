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
#ifndef LSST_QSERV_UTIL_CMD_PARSER_H
#define LSST_QSERV_UTIL_CMD_PARSER_H

/// CmdParser.h declares:
///
/// class CmdParser
/// (see individual class documentation for more information)

// System headers

#include <map>
#include <ostream>
#include <set>
#include <string>
#include <vector>

// Qserv headers

// This header declarations

namespace lsst {
namespace qserv {
namespace util {

/**
 * The command line parser class for the command-line applications.
 * It helps with parsing and interpreting command line arguments into
 * positional parameters, flags and options:
 *
 *   <parameter>
 *   --<flag>
 *   --<option>=<value>
 *
 * Parameters, flags and options can interleave in any order:
 *
 *   --flag1 <pos1> --opt1=val1 <pos2> <pos3> ...
 *
 * The constructor will throw exception std::invalid_argument if non-conforming
 * arguments will be found in the command line, such as:
 *
 *   --
 *   --<option>=
 */
class CmdParser {

public:

    /**
     * Return 'true' if the specified string is found in the collection.
     *
     * @param val - a string to be evaluated
     * @param col - a collection
     */
    static bool found_in (std::string const&              val,
                          std::vector<std::string> const& col);

    // Default construction and copy semantics are proxibited

    CmdParser () = delete;
    CmdParser (CmdParser const&) = delete;
    CmdParser& operator= (CmdParser const&) = delete;

    /**
     * Constructor
     *
     * @param argc  - the total number of rguments
     * @param argv  - the vector of arguments
     * @param usage - the command line argument syntax to be printed
     */
    CmdParser (int                argc,
               const char* const* argv,
               const char*        usage);

    /// Destructor
    virtual ~CmdParser ();

    /**
     * Return 'true' if the specified flag was found in the command line
     *
     * @param name - the name of a flag
     * @param 
     */
    bool flag (std::string const& name) const;

    /**
     * Check if the specified option was found in the command line, parse its
     * value into a result of the boolean type and return it. Return the default
     * value if the option was not found. Throw exception std::invalid_argument
     * if the type conversion is not possible.
     *
     * @param name         - the name of a flag
     * @param defaultValue - a default value of the option
     */
    template <class V>
    V option (std::string const& name,
             V const&            defaultValue) const {

        return optionImpl (name, defaultValue);
    }

    /**
     * Return a collection of positional parameters (except the command name itself)
     * translated into the specified type. Throw exception std::invalid_argument if
     * the type conversion is not possible.
     *
     * @param vals     - a collection of values
     * @param posBegin - starting position (default: 1)
     */
    template <class V>
    void parameters (std::vector<V>& vals, unsigned int posBegin=1) const {
        // Skipping the command path
        size_t const size = _parameter.size();
        if (posBegin >= size) {
            vals.resize(0);
            return;
        }
        vals.resize(size-posBegin);
        for (size_t pos=posBegin; pos < size; ++pos)
            parameterImpl(pos, vals[pos-1]);
    }

    /// @return collection of positional parameters (except the command name itself)
    template <class V>
    std::vector<V> parameters (unsigned int posBegin=1) const {
        std::vector<V> vals;
        parameters(vals, posBegin);
        return vals;
    }

    /**
     * Return a value of a parameter at the specified position
     * translated into the specified type. Throw exception std::out_of_range
     * if there are fewer arguments than the specified position. Throw
     * exception std::invalid_argument if the type conversion is not possible.
     *
     * @param pos - position number starting with 0
     */
    template <class V>
    V parameter (unsigned int pos) const {
        V val;
        parameterImpl (pos, val);
        return val;
    }

    /**
     * Return a string value of a parameter where only a limited
     * choise of (case sensitive) values is allowed. Throw exception
     * std::invalid_argument if the parameter is not found among
     * the allowed ones.
     *
     * @param pos           - position number starting with 0
     * @param allowedValues - a collection of possible values
     */
    std::string parameterRestrictedBy (unsigned int                    pos,
                                       std::vector<std::string> const& allowedValues) const;

    /**
     * Dump parsed flags, options and parameters into the specified stream
     *
     * @param os - an output stream
     */
    void dump (std::ostream& os) const;

private:

    /// Parse the command line
    void parse ();

    /**
     * Implement option lookup for the boolean type
     *
     * @see method CmdParser::option()
     */
    bool optionImpl (std::string const& name,
                    bool const&         defaultValue) const;

    /**
     * Implement option lookup for the integer type
     *
     * @see method CmdParser::option()
     */
    int optionImpl (std::string const& name,
                    int const&         defaultValue) const;

    /**
     * Implement option lookup for the integer type
     *
     * @see method CmdParser::option()
     */
    unsigned int optionImpl (std::string const&  name,
                             unsigned int const& defaultValue) const;
 
    /**
     * Implement option lookup for the string type
     *
     * @see method CmdParser::option()
     */
    std::string optionImpl (std::string const& name,
                            std::string const& defaultValue) const;

    /**
     * Implement positional parameter lookup for the boolean type
     *
     * @param pos - position number starting with 0
     * @param val - a value to be set upon the successfull completion
     *              of the method
     * 
     * @see method CmdParser::parameter()
     */
    void parameterImpl (unsigned int pos,
                        bool&        val) const;

    /**
     * Implement positional parameter lookup for the integer type
     *
     * @param pos - position number starting with 0
     * @param val - a value to be set upon the successfull completion
     *              of the method
     * 
     * @see method CmdParser::parameter()
     */
    void parameterImpl (unsigned int pos,
                        int&         val) const;

    /**
     * Implement positional parameter lookup for the unsigned integer type
     *
     * @param pos - position number starting with 0
     * @param val - a value to be set upon the successfull completion
     *              of the method
     * 
     * @see method CmdParser::parameter()
     */
    void parameterImpl (unsigned int  pos,
                        unsigned int& val) const;

    /**
     * Implement positional parameter lookup for the string type
     *
     * @param pos - position number starting with 0
     * @param val - a value to be set upon the successfull completion
     *              of the method
     * 
     * @see method CmdParser::parameter()
     */
    void parameterImpl (unsigned int pos,
                        std::string& val) const;

private:
    
    /// Command line arguments to be parsed
    std::vector<std::string> _argv;

    /// The syntax string
    std::string _usage;

    /// Flags
    std::set<std::string> _flag;

    /// Options and their values
    std::map<std::string, std::string> _option;

    /// Positional parameters
    std::vector<std::string> _parameter;

};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_CMD_PARSER_H