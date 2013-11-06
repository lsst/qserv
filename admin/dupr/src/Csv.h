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

/// \file
/// \brief Reading and writing of CSV-like data formats.

#ifndef LSST_QSERV_ADMIN_DUPR_CSV_CSV_H
#define LSST_QSERV_ADMIN_DUPR_CSV_CSV_H

#include <sys/types.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "boost/mpl/assert.hpp"
#include "boost/mpl/or.hpp"
#include "boost/program_options.hpp"
#include "boost/scoped_array.hpp"
#include "boost/type_traits/is_integral.hpp"
#include "boost/type_traits/is_floating_point.hpp"
#include "boost/type_traits/is_same.hpp"
#include "boost/unordered_map.hpp"

#include "Constants.h"


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

namespace csv {

/// A description of a CSV dialect. The following assumptions must hold:
///
/// - A record must be contained in exactly one line. Fields
///   with embedded line terminators are not parsed correctly.
/// - Lines are terminated by LF, CR, or CRLF.
/// - No line may be longer than MAX_LINE_SIZE bytes.
/// - No field may be longer than MAX_FIELD_SIZE bytes.
/// - The character encoding must use byte sized code values and be ASCII
///   based (for example, ASCII, Latin-1 and UTF-8 are supported, but
///   UTF-16 is not). Additionally, if a multi-byte character encoding is
///   in use, then the delimiter, quote and escape characters must be
///   representable by a single byte.
///
/// When reading input, escape characters are dropped and the following
/// character is usually returned as-is. The following exceptions apply
/// (assuming the escape character is '\'):
///
/// - '\0' is translated to ASCII NUL.
/// - '\b' is translated to ASCII backspace.
/// - '\f' is translated to ASCII form feed.
/// - '\n' is translated to ASCII newline.
/// - '\r' is translated to ASCII carriage return.
/// - '\t' is translated to ASCII horizontal tab.
/// - '\v' is translated to ASCII vertical tab.
/// - '\Z' is translated to ASCII substitute.
///
/// If a field value begins with a quote, it is considered to be quoted.
/// Quoted fields are stripped of their enclosing quotes, and embedded
/// sequences of two quotes are collapsed to a single quote. The quote
/// character has no special significance for unquoted fields.
///
/// When writing output with escaping enabled, embedded CR, LF, quote, escape
/// and delimiter characters are escaped. If escaping is disabled but quoting
/// is enabled, then fields containing an embedded delimiter or quote are
/// quoted and embedded quotes are doubled. A field with an embedded CR or LF
/// cannot be written unless escaping is enabled. If neither quoting nor
/// escaping is enabled, fields containing embedded CR, LF, or delimiter
/// characters cannot be written.
///
/// Note that the quote, escape, and delimiter characters must be distinct, and
/// are not allowed to be CR, LF or NUL. If escaping is enabled, then the quote,
/// escape and delimiter characters cannot be set to any character in the string
/// '0bfnrtvNZ' (to avoid ambiguity with the standard escape sequences above).
class Dialect {
public:
    /// Create a dialect with an explicit NULL string.
    /// To disable quoting, specify '\0' as the quote character. To disable
    /// escaping, specify '\0' as the escape character.
    Dialect(std::string const & null,
            char delimiter,
            char escape,
            char quote);

    /// Create a dialect. The NULL string is set to "NULL" if quoting
    /// is enabled, "\N" if escaping enabled, and "" otherwise. To disable
    /// quoting, specify '\0' as the quote character. To disable escaping,
    /// specify '\0' as the escape character.
    Dialect(char delimiter,
            char escape,
            char quote);

    /// Build a dialect from configuration variables with names given by the
    /// concatenation of prefix and "null", "delimiter", "escape",
    /// "no-escape", "quote" and "no-quote".
    Dialect(boost::program_options::variables_map const & vm,
            std::string const & prefix);

    Dialect(Dialect const & dialect);

    ~Dialect();

    Dialect & operator=(Dialect const & dialect);

    std::string const & getNull() const { return _null; }

    char getDelimiter() const { return _delimiter; }
    char getEscape() const { return _escape; }
    char getQuote() const { return _quote; }

    bool operator==(Dialect const & d) const {
        return _null == d._null &&
               _delimiter == d._delimiter &&
               _escape == d._escape &&
               _quote == d._quote;
    }

    /// Is the encoded field value identical to the NULL string?
    bool isNull(char const * value, size_t size) const {
        return _null.compare(0, _null.size(), value, size) == 0;
    }
    /// Decode a value encoded in this dialect into `buf` and return the
    /// number of characters written. No more than MAX_FIELD_SIZE characters
    /// are written - if more are required an exception is thrown. Leading
    /// and trailing whitespace is preserved.
    size_t decode(char * buf, char const * value, size_t size) const;
    /// Decode a field encoded in this dialect.
    std::string const decode(char const * value, size_t size) const {
        char buf[MAX_FIELD_SIZE];
        size = decode(buf, value, size);
        return std::string(buf, size);
    }

    /// Encode a field according to this dialect into `buf` and return the
    /// number of characters written. No more than MAX_FIELD_SIZE characters
    /// are written - if more are required an exception is thrown.
    size_t encode(char * buf, char const * value, size_t size) const;
    /// Encode a value in this dialect.
    std::string const encode(char const * value, size_t size) const {
        char buf[MAX_FIELD_SIZE];
        size = encode(buf, value, size);
        return std::string(buf, size);
    }

    /// Define configuration variables for specifying a dialect.
    static void defineOptions(
        boost::program_options::options_description & opts,
        std::string const & prefix);

private:
    static size_t const NUM_CHARS = 256; // Number of distinct character values.
    static std::string const _prohibited;
    static uint8_t const _unescape[NUM_CHARS];

    enum {
        HAS_CRLF   = 0x1,
        HAS_DELIM  = 0x2,
        HAS_QUOTE  = 0x4,
        HAS_ESCAPE = 0x8
    };

    int _scan(char const * value, size_t size) const;
    void _validate();

    std::string _null;
    // Map from character values to HAS_xxx flags.
    boost::scoped_array<uint8_t> _scanLut;
    bool _nullHasSpecial;
    char _delimiter;
    char _escape;
    char _quote;
};


/// \brief A class for producing an output CSV record from an input CSV record.
///
/// An Editor is constructed by specifying input and output CSV dialects, a
/// list of input field names, and a list of output field names. An output
/// field name matching an input field name causes the input field to appear
/// in the output record. Input fields with names not present in the output
/// field name list are dropped. Output field names not present in the input
/// receive a default value of NULL. There is no requirement for output field
/// names to be unique, though input field names must be.
///
/// Prior to the first `readRecord()` call, all input and output fields have
/// their values set to NULL. Output fields can be modified via `set()` and
/// `setNull()`. To load new input fields and clear out any edits, supply a
/// line of text to `readRecord()`. Subsequently, input fields for the line
/// can be accessed via `get()` and `isNull()`. An output record that combines
/// input field values and any edits performed since the last `readRecord()`
/// call or editor creation is written by `writeRecord()`.
///
/// Fields are formatted and/or type-converted on every field access or
/// modification, so performance sensitive code should cache field values.
///
/// Fields can be referred to by name or index. Field access via `get()` will
/// result in an exception if the field name or index is invalid or the field
/// value is NULL. Field modification via `set()` and `setNull()` has no effect
/// if the field name or index is invalid, or if the field is not in the output
/// field list. Referring to fields by index is faster than by name - use
/// `getFieldIndex()` to map field names to indexes ahead of time.
///
/// Together, these features allow an Editor to clone, reorder, modify and
/// drop CSV fields, while simultaneously performing CSV format conversion.
class Editor {
public:
    Editor(Dialect const & inputDialect,
           Dialect const & outputDialect,
           std::vector<std::string> const & inputFieldNames,
           std::vector<std::string> const & outputFieldNames);

    Editor(boost::program_options::variables_map const & vm);

    ~Editor();

    // -- Record input/output ----

    /// Set the input record to the first line in `[begin, end)` and return a
    /// pointer to the first character of the following line or `end`. The
    /// input line must remain live until the next call to `readRecord()`
    /// or editor destruction, whichever comes first. Raw input is never
    /// modified.
    char const * readRecord(char const * begin, char const * end);

    /// Write the combination of the current input fields and any edits
    /// performed to `buf`, returning a pointer to the character following the
    /// last character written. At most `MAX_LINE_SIZE` bytes are written -
    /// if the output record is longer, an exception is thrown.
    char * writeRecord(char * buf) const;

    // -- Metadata ----

    Dialect const & getInputDialect()  const { return _inputDialect; }
    Dialect const & getOutputDialect() const { return _outputDialect; }

    /// Return the number of input fields `readRecord()` expects to find in
    /// a line of text.
    int getNumInputFields() const { return _numInputFields; }
    /// Return an index for the named field or -1 if no such field exists.
    int getFieldIndex(std::string const & name) const {
        FieldMap::const_iterator i = _fieldMap.find(name);
        return i == _fieldMap.end() ? -1 : i->second;
    }

    ///@{
    /// Is the given field an input field?
    bool isInputField(int i) const {
        return i >= 0 && i < _numInputFields;
    }
    bool isInputField(std::string const & name) const {
        return isInputField(getFieldIndex(name));
    }
    ///@}

    // -- Field access ----

    ///@{
    /// Return true if the input field value is NULL or `i` is not a valid input
    /// field index. A field is NULL when it's encoded value matches the input
    /// dialect's NULL string exactly.
    bool isNull(int i) const {
        if (i < 0 || i >= _numInputFields) {
            return true;
        }
        Field const & f = _fields[i];
        return _inputDialect.isNull(f.inputValue, f.inputSize);
    }
    bool isNull(std::string const & name) const {
        return isNull(getFieldIndex(name));
    }
    ///@}

    ///@{
    /// Return the value of an input field value as a string. The decode flag
    /// controls whether the encoded value is decoded prior to return.
    std::string const get(int i, bool decode) const;
    std::string const get(std::string const & name, bool decode) const {
        return get(getFieldIndex(name), decode);
    }
    ///@}

    ///@{
    /// Return the decoded and type converted value of an input field.
    template <typename T> T get(int i) const {
        BOOST_MPL_ASSERT(( boost::mpl::or_<
            boost::is_integral<T>,
            boost::is_floating_point<T>
        > ));
        return _get<T>(i);
    }
    template <typename T> T get(std::string const & name) const {
        return get<T>(getFieldIndex(name));
    }
    ///@}

    // -- Field modification ----

    ///@{
    /// Set the value of an output field to NULL. Return true if the field was
    /// set, and false if it is not an output field and cannot be modified.
    bool setNull(int i);
    bool setNull(std::string const & name) {
        return setNull(getFieldIndex(name));
    }
    ///@}

    ///@{
    /// Set the value of an output field. Return true if the field was set,
    /// and false if it is not an output field and cannot be modified.
    bool set(int i, std::string const & value);
    bool set(int i, bool value) { return set(i, value ? '\1' : '\0'); }
    bool set(int i, char value);
    bool set(int i, int value);
    bool set(int i, long value);
    bool set(int i, long long value);
    bool set(int i, unsigned int value);
    bool set(int i, unsigned long value);
    bool set(int i, unsigned long long value);
    bool set(int i, float value);
    bool set(int i, double value);

    template <typename T> bool set(std::string const & name, T value) {
        return set(getFieldIndex(name), value);
    }
    ///@}

    /// Define configuration variables for CSV editing.
    static void defineOptions(
        boost::program_options::options_description & opts);

private:
    // Disable copy construction and assignment.
    Editor(Editor const &);
    Editor & operator=(Editor const &);

    typedef std::pair<char const *, char const *> CharConstPtrPair;
    typedef boost::unordered_map<std::string, int> FieldMap;

    struct Field {
        static uint16_t const DECODE = 0x01;
        static uint16_t const EDITED = 0x02;

        char const * inputValue;
        char * outputValue;
        uint16_t inputSize;
        uint16_t outputSize;
        uint16_t flags;

        Field();
        ~Field();
    };

    void _initialize(std::vector<std::string> const & inputFieldNames,
                     std::vector<std::string> const & outputFieldNames);
    CharConstPtrPair const _getFieldText(int i, char * buf) const;
    template <typename T> T _get(int i) const;

    Dialect const _inputDialect;
    Dialect const _outputDialect;
    bool const _dialectsMatch;
    int _numInputFields;
    int _numOutputFields;
    int _numFields;

    boost::scoped_array<Field> _fields;
    boost::scoped_array<int> _outputs;
    FieldMap _fieldMap;
};

template <> inline std::string Editor::get<std::string>(int i) const {
    return get(i, true);
}

} // namespace csv

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_CSV_CSV_H
