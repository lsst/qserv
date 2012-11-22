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

#include "Csv.h"

#include <ctype.h>
#include <errno.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

using std::bad_alloc;
using std::free;
using std::malloc;
using std::memcpy;
using std::memset;
using std::numeric_limits;
using std::pair;
using std::runtime_error;
using std::snprintf;
using std::string;
using std::vector;

namespace po = boost::program_options;


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

namespace csv {

// -- Dialect implementation ----

// When escaping is turned on, the escape, quote, and delimiter characters
// must not be set to any of these characters.
string const Dialect::_prohibited = "0bfnrtvNZ";

// Character unescape lookup table. For example, this maps the 'n' in the
// "\n" sequence to the LF character. There is one entry for every possible
// character value.
uint8_t const Dialect::_unescape[Dialect::NUM_CHARS] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
    0x00, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f,
    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f,
    0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x1a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f,
    0x60, 0x61, 0x08, 0x63, 0x64, 0x65, 0x0c, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x0a, 0x6f,
    0x70, 0x71, 0x0d, 0x73, 0x09, 0x75, 0x0b, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f,
    0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f,
    0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
    0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf,
    0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf,
    0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf,
    0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf,
    0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb, 0xec, 0xed, 0xee, 0xef,
    0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff,
};

Dialect::Dialect(char delimiter,
                 char escape,
                 char quote) :
    _null(),
    _scanLut(new uint8_t[NUM_CHARS]),
    _delimiter(delimiter),
    _escape(escape),
    _quote(quote)
{
    if (_quote != '\0') {
        _null = "NULL";
    } else if (_escape != '\0') {
        _null += _escape;
        _null += 'N';
    }
    _validate();
}

Dialect::Dialect(string const & null,
                 char delimiter,
                 char escape,
                 char quote) :
    _null(null),
    _scanLut(new uint8_t[NUM_CHARS]),
    _delimiter(delimiter),
    _escape(escape),
    _quote(quote)
{
    _validate();
}

Dialect::Dialect(po::variables_map const & vm, string const & prefix) :
    _null(),
    _scanLut(new uint8_t[NUM_CHARS])
{
    _delimiter = vm[prefix + "delimiter"].as<char>();
    if (vm.count(prefix + "no-quote") != 0) {
        _quote = '\0';
    } else {
        _quote = vm[prefix + "quote"].as<char>();
    }
    if (vm.count(prefix + "no-escape") != 0) {
        _escape = '\0';
    } else {
        _escape = vm[prefix + "escape"].as<char>();
    }
    if (vm.count(prefix + "null") != 0) {
        _null = vm[prefix + "null"].as<string>();
    } else if (_quote != '\0') {
        _null = "NULL";
    } else if (_escape != '\0') {
        _null += _escape;
        _null += 'N';
    }
    _validate();
}

Dialect::Dialect(Dialect const & dialect) :
    _null(dialect._null),
    _scanLut(new uint8_t[NUM_CHARS]),
    _nullHasSpecial(dialect._nullHasSpecial),
    _delimiter(dialect._delimiter),
    _escape(dialect._escape),
    _quote(dialect._quote)
{
    memcpy(_scanLut.get(), dialect._scanLut.get(), NUM_CHARS * sizeof(uint8_t));
}

Dialect::~Dialect() { }

Dialect & Dialect::operator=(Dialect const & dialect) {
    if (this != &dialect) {
        _null = dialect._null;
        _nullHasSpecial = dialect._nullHasSpecial;
        _delimiter = dialect._delimiter;
        _escape = dialect._escape;
        _quote = dialect._quote;
        memcpy(_scanLut.get(), dialect._scanLut.get(),
               NUM_CHARS * sizeof(uint8_t));
    }
    return *this;
}

size_t Dialect::decode(char * buf, char const * value, size_t size) const {
    if (_quote == '\0' && _escape == '\0') {
        if (size > MAX_FIELD_SIZE) {
            throw runtime_error("CSV field value is too long to decode.");
        }
        memcpy(buf, value, size);
        return size;
    } else if (_null.compare(0, _null.size(), value, size) == 0) {
        memcpy(buf, _null.data(), _null.size());
        return _null.size();
    }
    bool const quoted = (size > 0 && _quote != '\0' && value[0] == _quote);
    if (quoted) {
        ++value;
        --size;
    }
    size_t i = 0, j = 0;
    for (; i < size && j < MAX_FIELD_SIZE; ++j) {
        char c = value[i++];
        if (_escape != '\0' && c == _escape) {
            if (i < size) {
                c = static_cast<char>(
                    _unescape[static_cast<uint8_t>(value[i++])]);
            }
        } else if (quoted && c == _quote) {
            if (i < size) {
                // Collapse quote-quote to quote.
                if (value[i] == _quote) {
                    ++i;
                }
            } else {
                // Ignore the trailing quote character.
                break;
            }
        }
        buf[j] = c;
    }
    if (i < size) {
        throw runtime_error("CSV field value is too long to decode.");
    }
    return j;
}

size_t Dialect::encode(char * buf, char const * value, size_t size) const {
    if (value == 0) {
        memcpy(buf, _null.data(), _null.size());
        return _null.size();
    }
    int const flags = _scan(value, size);
    if (flags == 0) {
        if (!_nullHasSpecial && isNull(value, size)) {
            if (_quote != '\0') {
                // Quote value to avoid ambiguity with the NULL string.
                if (size > MAX_FIELD_SIZE - 2) {
                    throw runtime_error("CSV field value is too long to "
                                        "encode.");
                }
                buf[0] = _quote;
                memcpy(buf + 1, value, size);
                buf[size + 1] = _quote;
                size += 2;
            } else {
                throw runtime_error("Ambiguous CSV field value: the encoded "
                                    "value matches the dialect's NULL "
                                    "string exactly.");
            }
        } else {
            if (size > MAX_FIELD_SIZE) {
                throw runtime_error("CSV field value is too long to "
                                    "encode.");
            }
            memcpy(buf, value, size);
        }
        return size;
    }
    size_t i = 0, j = 0;
    if (_escape != '\0') {
        for (; i < size && j < MAX_FIELD_SIZE; ++i, ++j) {
            char c = value[i];
            if (c == '\r') {
                buf[j++] = _escape;
                if (j == MAX_FIELD_SIZE) {
                    break;
                }
                c = 'r';
            } else if (c == '\n') {
                buf[j++] = _escape;
                if (j == MAX_FIELD_SIZE) {
                    break;
                }
                c = 'n';
            } else if (c == _delimiter || c == _escape ||
                       (c == _quote && c != '\0')) {
                buf[j++] = _escape;
                if (j == MAX_FIELD_SIZE) {
                    break;
                }
            }
            buf[j] = c;
        }
    } else if (_quote != '\0') {
        if ((flags & HAS_CRLF) != 0) {
            throw runtime_error("Cannot encode CSV field with embedded "
                                "CR or LF characters in this dialect.");
        }
        // value contains embedded delimiter or quote character(s)
        buf[0] = _quote;
        for (j = 1; i < size && j < MAX_FIELD_SIZE - 1; ++i, ++j) {
            char c = value[i];
            if (c == _quote) {
               // Double-up embedded quotes.
               buf[j++] = c;
               if (j == MAX_FIELD_SIZE - 1) {
                   break;
               }
            }
            buf[j] = c;
        }
        buf[j++] = _quote;
    } else {
        throw runtime_error("Cannot encode CSV field with embedded CR, "
                            "LF or delimiter characters in this dialect.");
    }
    if (i < size) {
        throw runtime_error("CSV field value is too long to encode.");
    }
    return j;
}

void Dialect::defineOptions(po::options_description & opts,
                            string const & prefix)
{
    opts.add_options()
        ((prefix + "null").c_str(), po::value<string>(),
         "NULL CSV field value string. Leaving this option unspecified "
         "results in a dialect specific default - if quoting is enabled, "
         "NULL is used. Otherwise, if escaping is enabled, \\N is used "
         "(assuming \\ is the escape character). If neither is enabled, "
         "an empty string is used.")
        ((prefix + "delimiter").c_str(), po::value<char>()->default_value('\t'),
         "CSV field delimiter character. Cannot be '\\n' or '\\r'.")
        ((prefix + "quote").c_str(), po::value<char>()->default_value('"'),
         "CSV field quoting character.")
        ((prefix + "no-quote").c_str(), po::value<string>()->default_value(""),
         "Disable CSV field quoting.")
        ((prefix + "escape").c_str(), po::value<char>()->default_value('\\'),
         "CSV escape character.")
        ((prefix + "no-escape").c_str(), po::value<string>()->default_value(""),
         "Disable CSV character escaping.");
}

// Scan the given string for occurrences of the CR, LF, escape, quote or
// delimiter characters, and return a bitwise or of the HAS_xxx constants
// indicating which were found.
int Dialect::_scan(char const * value, size_t size) const {
    uint8_t flags = 0;
    for (size_t i = 0; i < size; ++i) {
        uint8_t c = static_cast<uint8_t>(value[i]);
        flags |= _scanLut[c];
    }
    return flags;
}

void Dialect::_validate() {
    // Perform sanity checks.
    if (_null.size() > MAX_FIELD_SIZE) {
        throw runtime_error("The CSV NULL representation is too long.");
    }
    // Make sure the delimiter, quote, and escape characters are distinct
    // and legal.
    if (_delimiter == '\0' || _delimiter == '\n' || _delimiter == '\r') {
        throw runtime_error("The CSV field delimiter may not be set to "
                            "NUL, CR or LF.");
    }
    if (_escape == _delimiter || _escape == '\n' || _escape == '\r') {
        throw runtime_error("The CSV escape character may not be set to "
                            "CR, LF or the delimiter character.");
    }
    if (_quote == _delimiter || _quote == '\n' || _quote == '\r') {
        throw runtime_error("The CSV field quoting character may not be set "
                            "to CR, LF or the delimiter character.");
    }
    if (_escape != '\0') {
        if (_escape == _quote) {
            throw runtime_error("The CSV escape and quote characters are "
                                "identical.");
        }
        if (_prohibited.find(_escape) != string::npos ||
            _prohibited.find(_quote) != string::npos ||
            _prohibited.find(_delimiter) != string::npos) {
            throw runtime_error("Escaping the CSV delimiter, quote, and/or "
                                "escape characters would produce a standard "
                                "escape sequence. Avoid characters from '" +
                                _prohibited + "' or disable escaping.");
        }
    }
    // Everything looks good, so populate the look-up-table used by _scan().
    memset(_scanLut.get(), 0, NUM_CHARS * sizeof(uint8_t));
    _scanLut[static_cast<int>('\r')]       = HAS_CRLF;
    _scanLut[static_cast<int>('\n')]       = HAS_CRLF;
    _scanLut[static_cast<int>(_delimiter)] = HAS_DELIM;
    _scanLut[static_cast<int>(_quote)]     = HAS_QUOTE;
    _scanLut[static_cast<int>(_escape)]    = HAS_ESCAPE;
    // Make sure the NULL representation is semi-sane.
    int flags = _scan(_null.data(), _null.size());
    if ((flags & (HAS_CRLF | HAS_DELIM)) != 0) {
        throw runtime_error("The CSV NULL representation must not contain "
                            "CR, LF, or delimiter characters.");
    }
    _nullHasSpecial = (flags != 0);
}


// -- Editor implementation ----

Editor::Field::Field() :
    inputValue(0), outputValue(0), inputSize(0), outputSize(0), flags(0)
{ }

Editor::Field::~Field() {
    inputValue = 0;
    if (outputValue) {
        free(outputValue);
        outputValue = 0;
    }
}

Editor::Editor(Dialect const & inputDialect,
               Dialect const & outputDialect,
               vector<string> const & inputFieldNames,
               vector<string> const & outputFieldNames) :
    _inputDialect(inputDialect),
    _outputDialect(outputDialect),
    _dialectsMatch(_inputDialect == _outputDialect),
    _numInputFields(static_cast<int>(inputFieldNames.size())),
    _numOutputFields(static_cast<int>(outputFieldNames.size())),
    _fields(new Field[inputFieldNames.size() + outputFieldNames.size()]),
    _outputs(new int[outputFieldNames.size()]),
    _fieldMap()
{
    _initialize(inputFieldNames, outputFieldNames);
}

Editor::Editor(po::variables_map const & vm) :
    _inputDialect(vm, "in.csv."),
    _outputDialect(vm, "out.csv."),
    _dialectsMatch(_inputDialect == _outputDialect),
    _fields(),
    _outputs(),
    _fieldMap()
{
    if (vm.count("in.csv.field") == 0) {
        throw runtime_error("Input CSV field names not specified.");
    }
    vector<string> const * inputFieldNames =
        &vm["in.csv.field"].as<vector<string> >();
    vector<string> const * outputFieldNames;
    if (vm.count("out.csv.field") == 0) {
        outputFieldNames = inputFieldNames;
    } else {
        outputFieldNames = &vm["out.csv.field"].as<vector<string> >();
    }
    _numInputFields = static_cast<int>(inputFieldNames->size());
    _numOutputFields = static_cast<int>(outputFieldNames->size()),
    _fields.reset(new Field[inputFieldNames->size() + outputFieldNames->size()]);
    _outputs.reset(new int[outputFieldNames->size()]);
    _initialize(*inputFieldNames, *outputFieldNames);
}

Editor::~Editor() { }

char const * Editor::readRecord(char const * const begin,
                                char const * const end)
{
    if (end <= begin || begin == 0) {
        throw runtime_error("Empty or invalid input line.");
    } else if (_numInputFields == 0) {
        throw runtime_error("Calling readRecord() is illegal unless at "
                            "least one CSV input field has been defined.");
    }
    bool quoted = false;
    bool escaped = false;
    bool decode = false;
    Field * f = _fields.get();
    Field * fend = f + _numInputFields;
    f->inputValue = begin;
    char const * cur = begin;
    for (; cur < end; ++cur) {
        char const c = *cur;
        if (c == '\n' || c == '\r') {
            break;
        } else if (c == '\0') {
            continue;
        }
        if (escaped) {
            // The previous character was an escape character.
            escaped = false;
        } else if (quoted) {
            // cur is inside a quoted field.
            if (c == _inputDialect.getEscape()) {
                escaped = true;
            } else if (c == _inputDialect.getQuote()) {
                // Skip over embedded double quotes. If c is the
                // last character in the input or is followed by
                // a delimiter or line terminator, then c is the
                // terminating quote of this field.
                if (cur + 1 < end) {
                    char const next = cur[1];
                    if (next == c) {
                        ++cur;
                    } else if (next == '\n' || next == '\r') {
                        ++cur;
                        quoted = false;
                        break;
                    } else if (next == _inputDialect.getDelimiter()) {
                        quoted = false;
                    }
                } else {
                    quoted = false;
                }
            }
        } else {
            if (c == _inputDialect.getEscape()) {
                escaped = true;
                decode = true;
            } else if (c == _inputDialect.getDelimiter()) {
                // End of current field reached.
                ptrdiff_t sz = cur - f->inputValue;
                if (sz > MAX_FIELD_SIZE) {
                    throw runtime_error("CSV field value is too long.");
                }
                f->inputSize = static_cast<uint16_t>(sz);
                f->outputSize = 0;
                f->flags = (decode ? Field::DECODE : 0);
                decode = false;
                // Advance to the next field.
                ++f;
                if (f == fend) {
                    throw runtime_error("CSV record contains more than the "
                                        "expected number of fields.");
                }
                f->inputValue = cur + 1;
                if (cur + 1 < end && cur[1] == _inputDialect.getQuote()) {
                    // The next field is quoted.
                    quoted = true;
                    decode = true;
                    ++cur;
                }
            }
        }
    }
    if (quoted || escaped) {
        throw runtime_error("CSV record contains an embedded line terminator, "
                            "a trailing escape character, or a quoted field "
                            "without a trailing quote character.");
    }
    if (f + 1 != fend) {
        throw runtime_error("CSV record contains less than the expected "
                            "number of fields.");
    }
    ptrdiff_t sz = cur - f->inputValue;
    if (sz > MAX_FIELD_SIZE) {
        throw runtime_error("CSV field value is too long.");
    }
    f->inputSize = static_cast<uint16_t>(sz);
    f->outputSize = 0;
    f->flags = (decode ? Field::DECODE : 0);
    // Set output values for remaining fields to NULL.
    fend = _fields.get() + _numFields;
    for (++f; f != fend; ++f) {
        string const & null = _outputDialect.getNull();
        memcpy(f->outputValue, null.data(), null.size());
        f->outputSize = static_cast<uint16_t>(null.size());
        f->flags = 0;
    }
    // Advance past the trailing line terminator character(s).
    if (cur < end) {
        char const c = *cur++;
        if (c == '\r' && cur < end && *cur == '\n') {
            ++cur;
        }
    }
    return cur;
}

char * Editor::writeRecord(char * buf) const {
    char decodeBuf[MAX_FIELD_SIZE];
    char encodeBuf[MAX_FIELD_SIZE];
    size_t size = 0;
    char const delimiter = _outputDialect.getDelimiter();

    for (int i = 0; i < _numOutputFields; ++i) {
        Field const & f = _fields[_outputs[i]];
        char const * val;
        size_t sz;
        if (!f.inputValue || (f.flags & Field::EDITED) != 0) {
            // Output values are always encoded in the output dialect.
            val = f.outputValue;
            sz = f.outputSize;
        } else if (_dialectsMatch) {
            // Input values are encoded in the output dialect.
            val = f.inputValue;
            sz = f.inputSize;
        } else {
            // CSV format conversion is necessary.
            if (_inputDialect.isNull(f.inputValue, f.inputSize)) {
                val = _outputDialect.getNull().data();
                sz = _outputDialect.getNull().size();
            } else {
                if ((f.flags & Field::DECODE) != 0) {
                    // Decode input.
                    sz = _inputDialect.decode(
                        decodeBuf, f.inputValue, f.inputSize);
                    val = decodeBuf;
                } else {
                    val = f.inputValue;
                    sz = f.inputSize;
                }
                sz = _outputDialect.encode(encodeBuf, val, sz);
                val = encodeBuf;
            }
        }
        // Make sure never to write more than MAX_LINE_SIZE characters.
        if (i > 0) {
            buf[size++] = delimiter;
        }
        if (size + sz > MAX_LINE_SIZE - 1) {
            throw runtime_error("Output CSV record is longer than the "
                                "maximum supported line length.");
        }
        memcpy(buf + size, val, sz);
        size += sz;
    }
    buf[size++] = '\n';
    return buf + size;
}

string const Editor::get(int i, bool decode) const {
    if (i < 0 || i >= _numInputFields) {
        throw runtime_error("Invalid input field.");
    }
    Field const & f = _fields[i];
    char const * val = f.inputValue;
    size_t sz = f.inputSize;
    if (decode) {
        if (_inputDialect.isNull(val, sz)) {
            throw runtime_error("Input field value is NULL.");
        }
        if ((f.flags & Field::DECODE) != 0) {
            return _inputDialect.decode(val, sz);
        }
    }
    return string(val, sz);
}

bool Editor::setNull(int i) {
    if (i < 0 || i >= _numFields) {
        return false;
    }
    Field * f = &_fields[i];
    if (!f->outputValue) {
        return false;
    }
    string const & null = _outputDialect.getNull();
    memcpy(f->outputValue, null.data(), null.size());
    f->outputSize = static_cast<uint16_t>(null.size());
    f->flags |= Field::EDITED;
    return true;
}

bool Editor::set(int i, string const & val) {
    if (i < 0 || i >= _numFields) {
        return false;
    }
    Field * f = &_fields[i];
    if (!f->outputValue) {
        return false;
    }
    f->outputSize = static_cast<uint16_t>(
        _outputDialect.encode(f->outputValue, val.data(), val.size()));
    f->flags |= Field::EDITED;
    return true;
}

bool Editor::set(int i, char c) {
    if (i < 0 || i >= _numFields) {
        return false;
    }
    Field * f = &_fields[i];
    if (!f->outputValue) {
        return false;
    }
    f->outputSize = static_cast<uint16_t>(
        _outputDialect.encode(f->outputValue, &c, 1));
    f->flags |= Field::EDITED;
    return true;
}

#define IMPLEMENT_SET_IMPL(U, V, format) \
bool Editor::set(int i, U val) { \
    char buf[MAX_FIELD_SIZE]; \
    if (i < 0 || i >= _numFields) { \
        return false; \
    } \
    Field * f = &_fields[i]; \
    if (!f->outputValue) { \
        return false; \
    } \
    int sz = snprintf(buf, sizeof(buf), "%" #format, static_cast<V>(val)); \
    assert(sz > 0 && sz < MAX_FIELD_SIZE); \
    f->outputSize = static_cast<uint16_t>( \
        _outputDialect.encode(f->outputValue, buf, static_cast<size_t>(sz))); \
    f->flags |= Field::EDITED; \
    return true; \
}

#define IMPLEMENT_SET(U, format) IMPLEMENT_SET_IMPL(U, U, format)

IMPLEMENT_SET(int, d)
IMPLEMENT_SET(long, ld)
IMPLEMENT_SET(long long, lld)
IMPLEMENT_SET(unsigned int, u)
IMPLEMENT_SET(unsigned long, lu)
IMPLEMENT_SET(unsigned long long, llu)

// These precisions are sufficient for lossless binary->decimal->binary
// round-tripping, assuming that conversions are correctly rounded and
// that float/double correspond to IEEE single/double precision floating
// point numbers.
IMPLEMENT_SET_IMPL(float, double, .9g)
IMPLEMENT_SET(double, .17g)

#undef IMPLEMENT_SET_IMPL
#undef IMPLEMENT_SET

void Editor::defineOptions(po::options_description & opts) {
    po::options_description in("\\___________ Input CSV format", 80);
    Dialect::defineOptions(in, "in.csv.");
    in.add_options()
        ("in.csv.field", po::value<vector<string> >(),
         "Input CSV field names, in order of occurrence. Specify this "
         "option as many times as there are input fields. Input field "
         "names must be unique.");
    po::options_description out("\\_________ Output CSV format", 80);
    Dialect::defineOptions(out, "out.csv.");
    out.add_options()
        ("out.csv.field", po::value<vector<string> >(),
         "Output CSV field names, in order of occurrence. To retain an "
         "input field in the output, include it in the output field list. "
         "There is no requirement that an input field be listed only once, "
         "or that the order of input and output fields match. To remove an "
         "input field from the output, simply omit it from the output field "
         "list. To introduce a new output field, specify a name not in the "
         "input field list - it will receive a default value of NULL.");
    opts.add(in).add(out);
}

void Editor::_initialize(vector<string> const & inputFieldNames,
                         vector<string> const & outputFieldNames)
{
    typedef pair<FieldMap::iterator, bool> Mapping;

    int i = 0; // total number of fields
    for (; i < _numInputFields; ++i) {
        string const & name = inputFieldNames[i];
        Mapping m = _fieldMap.insert(pair<string, int>(name, i));
        if (!m.second) {
            throw runtime_error("The input CSV field name list contains "
                                "duplicates.");
        }
        Field * f = &_fields[i];
        // Before the first readRecord() call, assign NULL to all input
        // fields.
        string const & null = _inputDialect.getNull();
        f->inputValue = null.data();
        f->inputSize = static_cast<uint16_t>(null.size());
    }
    for (int j = 0; j < _numOutputFields; ++j) {
        string const & name = outputFieldNames[j];
        Mapping m = _fieldMap.insert(pair<string, int>(name, i));
        if (m.second) {
            // The output field name does not match any input field. Create
            // a new output field and assign NULL to the output value.
            Field * f = &_fields[i];
            f->outputValue = static_cast<char *>(malloc(MAX_FIELD_SIZE));
            if (!f->outputValue) {
                throw bad_alloc();
            }
            string const & null = _outputDialect.getNull();
            memcpy(f->outputValue, null.data(), null.size());
            f->outputSize = static_cast<uint16_t>(null.size());
            _outputs[j] = i++;
        } else {
            // The output field name matched an existing field -
            // make sure space is available for an output value.
            int k = m.first->second;
            Field * f = &_fields[k];
            if (!f->outputValue) {
                // f is also an input field, so there is no need
                // to set an output value here.
                f->outputValue = static_cast<char *>(malloc(MAX_FIELD_SIZE));
                if (!f->outputValue) {
                    throw bad_alloc();
                }
            }
            _outputs[j] = k;
        }
    }
    _numFields = i;
}

template <> bool Editor::_get<bool>(int i) const {
    char buf[MAX_FIELD_SIZE];
    if (i < 0 || i >= _numInputFields) {
        throw runtime_error("Invalid input field");
    }
    Field const & f = _fields[i];
    char const * val = f.inputValue;
    size_t sz = f.inputSize;
    if (_inputDialect.isNull(val, sz)) {
        throw runtime_error("Input field value is NULL.");
    }
    // Decode field value if necessary.
    if ((f.flags & Field::DECODE) != 0) {
        sz = _inputDialect.decode(buf, val, sz);
        val = buf;
    }
    // Trim leading and trailing whitespace.
    char const * end = val + sz;
    for (; val < end && isspace(*val); ++val) { }
    for (; end > val && isspace(end[-1]); --end) { }
    // Binary/ASCII 0 are recognized as false,
    // binary/ASCII 1 are recognized as true.
    if (end - val == 1) {
        char c = val[0];
        if (c == '\0' || c == '0') {
            return false;
        }
        if (c == '\1' || c == '1') {
            return true;
        }
    }
    throw runtime_error("Failed to convert field value to a C++ bool.");
    return false; // unreachable
}

template <> char Editor::_get<char>(int i) const {
    char buf[MAX_FIELD_SIZE];
    if (i < 0 || i >= _numInputFields) {
        throw runtime_error("Invalid input field");
    }
    Field const & f = _fields[i];
    char const * val = f.inputValue;
    size_t sz = f.inputSize;
    if (_inputDialect.isNull(val, sz)) {
        throw runtime_error("Input field value is NULL.");
    }
    // Decode field value if necessary.
    if ((f.flags & Field::DECODE) != 0) {
        sz = _inputDialect.decode(buf, val, sz);
        val = buf;
    }
    if (sz != 1) {
        throw runtime_error("Failed to convert field to a C++ char.");
    }
    return val[0];
}

// There is a subtlety here. Since _get() specializations invoke strtoXXX(),
// the input string may need to be copied into a temporary buffer first.
// This is because the length of the string being operated on cannot be passed
// in, so those C functions will happily walk past the end of the field being
// converted. This can lead to crashes or incorrect results. For example,
// consider what happens when the field delimiter is a digit.

Editor::CharConstPtrPair const Editor::_getFieldText(int i, char * buf) const {
    if (i < 0 || i >= _numInputFields) {
        throw runtime_error("Invalid input field");
    }
    Field const & f = _fields[i];
    char const * val = f.inputValue;
    size_t sz = f.inputSize;
    if (_inputDialect.isNull(val, sz)) {
        throw runtime_error("Input field value is NULL.");
    }
    if (f.flags & Field::DECODE) {
        sz = _inputDialect.decode(buf, val, sz);
        val = buf;
        buf[sz] = '\0';
    }
    char const * end = val + sz;
    for (; val < end && isspace(*val); ++val) { }
    for (; end > val && isspace(end[-1]); --end) { }
    sz = static_cast<size_t>(end - val);
    if (sz == 0) {
        throw runtime_error("Cannot convert empty field to a value");
    }
    if (end == f.inputValue + f.inputSize) {
        memcpy(buf, val, sz);
        buf[sz] = '\0';
        val = buf;
        end = buf + sz;
    }
    return CharConstPtrPair(val, end);
}

#define SPECIALIZE_GET_INT(U, V, suffix) \
template <> U Editor::_get<U>(int i) const { \
    char buf[MAX_FIELD_SIZE + 1]; \
    CharConstPtrPair f = _getFieldText(i, buf); \
    char * e = 0; \
    errno = 0; \
    V v = strto ## suffix(f.first, &e, 10); \
    if (e != f.second) { \
        throw runtime_error("Cannot convert field value to a C++ " #U); \
    } else if ((errno == ERANGE) || \
               v > numeric_limits<U>::max() || \
               v < numeric_limits<U>::min()) { \
        throw runtime_error("Field value does not fit into a C++ " #U); \
    } \
    return static_cast<U>(v); \
}

#define SPECIALIZE_GET_FP(U, suffix) \
template <> U Editor::_get<U>(int i) const { \
    char buf[MAX_FIELD_SIZE + 1]; \
    CharConstPtrPair f = _getFieldText(i, buf); \
    char * e = 0; \
    U u = strto ## suffix(f.first, &e); \
    if (e != f.second) { \
        throw runtime_error("Cannot convert field value to a C++ " #U); \
    } \
    return u; \
}

SPECIALIZE_GET_INT(signed char, long, l)
SPECIALIZE_GET_INT(short, long, l)
SPECIALIZE_GET_INT(int, long, l)
SPECIALIZE_GET_INT(long, long, l)

SPECIALIZE_GET_INT(long long, long long, ll)

SPECIALIZE_GET_INT(unsigned char, unsigned long, ul)
SPECIALIZE_GET_INT(unsigned short, unsigned long, ul)
SPECIALIZE_GET_INT(unsigned int, unsigned long, ul)
SPECIALIZE_GET_INT(unsigned long, unsigned long, ul)

SPECIALIZE_GET_INT(unsigned long long, unsigned long long, ull)

SPECIALIZE_GET_FP(float, f)
SPECIALIZE_GET_FP(double, d)

#undef SPECIALIZE_GET_INT
#undef SPECIALIZE_GET_FP

} // namespace csv

}}}} // namespace lsst::qserv::admin::dupr
