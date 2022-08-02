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

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/protocol.pb.h"

// This header declarations
namespace lsst::qserv::replica::csv {

/// The maximum number of characters (including the terminator character) in a row.
constexpr size_t const MAX_ROW_LENGTH = 16 * 1024 * 1024;

/// The class DialectInput stores unprocessed input for the corresponding
/// parameters of class Dialect.
class DialectInput;

/**
 * The class Dialect stores parameters needed to correctly interpret
 * the CSV/TSV formatted input stream of bytes.
 *
 * @note The current implementation of the class only supports the most commonly
 *   used subset of the parameters' values. See further details in the implementation
 *   file for the class. See MySQL documentation on the usage and allowed values of
 *   the parameters at: https://dev.mysql.com/doc/refman/8.0/en/load-data.html
 */
class Dialect {
public:
    // Default values for the parameters are defined here to allow sharing them
    // with other applications (command line tools, etc.).
    static std::string const defaultFieldsTerminatedBy;
    static std::string const defaultFieldsEnclosedBy;
    static std::string const defaultFieldsEscapedBy;
    static std::string const defaultLinesTerminatedBy;

    // Allowed values for each parameter that are supported by the implementation.
    static std::vector<std::string> const allowedFieldsTerminatedBy;
    static std::vector<std::string> const allowedFieldsEnclosedBy;
    static std::vector<std::string> const allowedFieldsEscapedBy;
    static std::vector<std::string> const allowedLinesTerminatedBy;

    /**
     * The default constructor will initialize object members with the default
     * values of the parameters.
     */
    Dialect();

    /**
     * Translate string sequences into valid characters accepted by MySQL's
     * statement 'LOAD DATA INFILE'.
     * @note Values of the parameters can't be empty. Use the corresponding default
     *   values if needed.
     * @param dialectInput The unprocessed input for the corresponding parameters of the dialect.
     * @throws std::invalid_argument For incorrect input that can't be parsed into
     *   a subset of characters recognized by MySQL or into a restricted subset of
     *   the last one as implemented by the class.
     */
    explicit Dialect(DialectInput const& dialectInput);

    Dialect(Dialect const&) = default;
    Dialect& operator=(Dialect const&) = default;
    ~Dialect() = default;

    char fieldsTerminatedBy() const { return _fieldsTerminatedBy; }
    char fieldsEnclosedBy() const { return _fieldsEnclosedBy; }
    char fieldsEscapedBy() const { return _fieldsEscapedBy; }
    char linesTerminatedBy() const { return _linesTerminatedBy; }

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
     * @param quote The optional quote.
     * @return A string generated for values of the corresponding options stored
     *   in an object.
     */
    std::string sqlOptions(std::string const& quote = "'") const;

private:
    // In-memory representations of the parameters meant to be used in an implementation
    // of an algorithm parsing the CSV/TSV formatted input.
    char _fieldsTerminatedBy;
    char _fieldsEnclosedBy;
    char _fieldsEscapedBy;
    char _linesTerminatedBy;
};

/// @see class Dialect
class DialectInput {
public:
    /// Convert from the Protobuf object
    explicit DialectInput(ProtocolDialectInput const& obj);

    DialectInput() = default;
    DialectInput(DialectInput const&) = default;
    DialectInput& operator=(DialectInput const&) = default;
    ~DialectInput() = default;

    /// @return Protobuf representation of the object
    std::unique_ptr<ProtocolDialectInput> toProto() const;

    /// @return JSON representation of the object
    nlohmann::json toJson() const;

    std::string fieldsTerminatedBy = Dialect::defaultFieldsTerminatedBy;
    std::string fieldsEnclosedBy = Dialect::defaultFieldsEnclosedBy;
    std::string fieldsEscapedBy = Dialect::defaultFieldsEscapedBy;
    std::string linesTerminatedBy = Dialect::defaultLinesTerminatedBy;
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
     * found in the buffer. The parameter \p flush should be set to 'true' to report
     * the last non-terminated row (if any) stored in the parser. The value should
     * be set to 'true'
     * @param inBuf A pointer to the input buffer.
     * @param inBufSize The number of bytes to be read from the buffer (allowed to be 0).
     * @param flush If 'true' then invoke the callback (parameter \p onStringParsed) to report
     *   the last non-terminated line stored in the parser's buffer if the buffer is not empty.
     * @param onStringParsed The callback function to be called for each line parsed.
     */
    void parse(char const* inBuf, size_t inBufSize, bool flush,
               ParsedStringCallbackType const& onStringParsed);

    Dialect const& dialect() const { return _dialect; }

    /// @return The total number of lines parsed and reported to a client.
    size_t numLines() const { return _lineNum - 1; }

private:
    Dialect const _dialect;
    std::unique_ptr<char[]> _lineBuf;
    size_t _lineBufNext = 0;
    size_t _lineNum = 1;         ///< the number of the current line (for diagnostic messages)
    bool _inEscapeMode = false;  ///< for counting escapes while processing the input stream
};

}  // namespace lsst::qserv::replica::csv

#endif  // LSST_QSERV_REPLICA_CSV_H
