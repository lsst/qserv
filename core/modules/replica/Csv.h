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
#ifndef LSST_QSERV_REPLICA_CSV_H
#define LSST_QSERV_REPLICA_CSV_H

// System headers
#include <functional>
#include <memory>

// Qserv headers
#include "replica/Common.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace csv {

/// The maximum number of characters (including the terminator character) in a row.
constexpr size_t const MAX_ROW_LENGTH = 64 * 1024;

/**
 * The class Dialect stores parameters needed to correctly interpret
 * the CSV/TSV formatted input stream of bytes.
 */
class Dialect {
public:

    // Default values for the parameters are defined here to allow sharing them
    // with other applications (command line tools, etc.).
    static std::string const defaultFieldsTerminatedBy;
    static std::string const defaultFieldsEnclosedBy;
    static std::string const defaultFieldsEscapedBy;
    static std::string const defaultLinesTerminatedBy;

    /**
     * Translate string sequences into valid characters accepted by MySQL's
     * statement 'LOAD DATA INFILE'. See MySQL documentation on the usage and
     * allowed values of the parameters: https://dev.mysql.com/doc/refman/8.0/en/load-data.html
     * @note Values of the parameters can't be empty. The default values of the parameters
     *   corresponds to the ones in the MySQL documentation (see the link above).
     * @note The current implementation of the class only supports the most commonly
     *   used subset of the parameters' values. See further details in the implementation
     *   file for the class.
     * @param fieldsTerminatedBy_ A character separating fields within a row.
     * @param fieldsEnclosedBy_  A character quoting fields within a row.
     * @param fieldsEscapedBy_ A character which is used to escape special characters.
     * @param linesTerminatedBy_ A character terminating the lines.
     * @throws std::invalid_argument For incorrect input that can't be parsed into
     *   a subset of characters recognized by MySQL or into a restricted subseet of
     *   the last one as implemented by the class.
     */
    explicit Dialect(std::string const& fieldsTerminatedBy_=defaultFieldsTerminatedBy,
                     std::string const& fieldsEnclosedBy_=defaultFieldsEnclosedBy,
                     std::string const& fieldsEscapedBy_=defaultFieldsEscapedBy,
                     std::string const& linesTerminatedBy_=defaultLinesTerminatedBy);

    Dialect(Dialect const&) = default;
    Dialect& operator=(Dialect const&) = default;
    ~Dialect() = default;

    /**
     * Generate options for MySQL statement:
     * @code
     * LOAD DATA
     *     ...
     *     [{FIELDS | COLUMNS}
     *         [TERMINATED BY 'string']
     *         [[OPTIONALLY] ENCLOSED BY 'char']
     *         [ESCAPED BY 'char']
     *     ]
     *     [LINES
     *         [STARTING BY 'string']
     *         [TERMINATED BY 'string']
     *     ]
     *     ...
     * @code
     * @return A string generated for values of the corresponding options stored
     *   in an object.
     */
    std::string sqlOptions() const;

    // In-memory representations of the parameters meant to be used in an implementation
    // of an algorithm parsing the CSV/TSV formatted input.
    char const fieldsTerminatedBy;
    char const fieldsEnclosedBy;
    char const fieldsEscapedBy;
    char const linesTerminatedBy;
};


/**
 * The class Parser parses the CSV/TSV formatted input stream of bytes into rows
 * terminated according to the specified 'dialect'. The main purpose of the parser
 * is to prepare the rows for further post-processing (such as adding extra columns)
 * by the Ingest system before loading the processed rows into the destination table.
 *
 * @see class Dialect
 */
class Parser {
public:
    /**
     * The function type for notifications called on each line processed by
     * the parser. The function has two parameters:
     *   const char* - a pointer to the very first character of the line
     *   size_t      - the total length of the line including line terminator
     */
    typedef std::function<void(char const*, size_t)> ParsedStringCallbackType;

    /**
     * Construct an object of the class configured with the specified Dialect.
     * The dialect will be used when parsing the input stream.
     * @param dialect The dialect object.
     */
    explicit Parser(Dialect const& dialect);

    Parser() = delete;
    Parser(Parser const&) = delete;
    Parser& operator=(Parser const&) = delete;

    /**
     * Parse the input buffer and call the specified function for each properly
     * terminated (by the corresponding EOL sequence of the Dialect) row
     * found in the buffer. The parameter \flush should be set to 'true' to report
     * the last non-terminated fow (if any) stored in the parser. The value should
     * be set to 'true'
     * @param inBuf A pointer to the input buffer.
     * @param inBufSize The number of bytes to be read from the buffer (allowed to be 0).
     * @param flush If 'true' then invoke the callback (parameter \onStringParsed) to report
     *   the last line build from the content of the line buffer if the buffer is not empty.
     * @param onStringParsed The callback function to be called for each non-empty line
     *   parsed in the input buffer and (optionally, if the parameter \flash is set to 'true'
     *   for the last incomplete line).
     * @note Empty rows found in the buffer will be ignored. No callbacks will
     *   be made for them.
     */
    void parse(char const* inBuf,
               size_t inBufSize,
               bool flush,
               ParsedStringCallbackType const& onStringParsed);

    Dialect const& dialect() const { return _dialect; }

private:
    Dialect const _dialect;
    std::unique_ptr<char[]> _lineBuf;
    size_t _lineBufNext = 0;
    size_t _lineNum = 1;        ///< the number of the current line (for diagnostic messages) 
};

}}}} // namespace lsst::qserv::replica::csv

#endif // LSST_QSERV_REPLICA_CSV_H
