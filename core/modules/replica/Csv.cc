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
#include "replica/Csv.h"

// System headers
#include <algorithm>
#include <map>
#include <string.h>

using namespace std;

namespace {

string const context = "Csv::Parser: ";

map<string, char> const inTranslationMap = {
    {R"(\0)", '\0'},
    {R"(\t)", '\t'},
    {R"(,)",  ',' },
    {R"(')",  '\''},
    {R"(")",  '"' },
    {R"(\\)", '\\'},
    {R"(\n)", '\n'}
};

map<char, string> const outTranslationMap = {
    {'\0', R"()"  },
    {'\t', R"(\t)"},
    {',',  R"(,)" },
    {'\'', R"(\')"},
    {'"',  R"(")" },
    {'\\', R"(\\)"},
    {'\n', R"(\n)"}
};

/**
 * Translate the string value of the specified parameter into a character
 * given a subset of strings allowed in a contex of the parameter.
 * @param name The name of a parameter (used for error reporting).
 * @param value The input string value of the parameter to be translated.
 * @param allowedValues A collection of allowed input (string) values.
 * @return A character corresponding to the input string.
 * @throw logic_error For not providing any choices in the collection of \allowedValues.
 * @throw invalid_argument For incorrect input.
 */
char parseParam(string const& name,
                string const& value,
                vector<string> const& allowedValues) {
    if (allowedValues.empty()) {
        throw logic_error(
                context + "calling the method with empty choices to translate the value '" + value
                + "' of the parameter '" + name + "'.");
    }
     if (value.empty()) {
        throw invalid_argument(context + "a value of the parameter '" + name + "' is empty.");
    }
   if (allowedValues.cend() == find(allowedValues.cbegin(), allowedValues.cend(), value)) {
        throw invalid_argument(
                context + "the value '" + value + "' of the parameter '" + name + "'."
                + " is not allowed for the given parameter."
        );
    }
    auto const itr = inTranslationMap.find(value);
    if (inTranslationMap.cend() == itr) {
        throw invalid_argument(
                context + "the value '" + value + "' of the parameter '" + name + "'."
                + " is not supported by the Parser."
        );
    }
    return itr->second;
}
}

namespace lsst {
namespace qserv {
namespace replica {
namespace csv {

string const Dialect::defaultFieldsTerminatedBy = R"(\t)";
string const Dialect::defaultFieldsEnclosedBy = R"(\0)";    // The special value to indicate a lack of enclosing characters
string const Dialect::defaultFieldsEscapedBy = R"(\\)";
string const Dialect::defaultLinesTerminatedBy= R"(\n)";


Dialect::Dialect(string const& fieldsTerminatedBy_,
                 string const& fieldsEnclosedBy_,
                 string const& fieldsEscapedBy_,
                 string const& linesTerminatedBy_)
    :   fieldsTerminatedBy(::parseParam("fieldsTerminatedBy", fieldsTerminatedBy_, {R"(\t)", R"(,)"})),
        fieldsEnclosedBy(  ::parseParam("fieldsEnclosedBy",   fieldsEnclosedBy_,   {R"(\0)", R"(')", R"(")"})),
        fieldsEscapedBy(   ::parseParam("fieldsEscapedBy",    fieldsEscapedBy_,    {R"(\\)"})),
        linesTerminatedBy( ::parseParam("linesTerminatedBy",  linesTerminatedBy_,  {R"(\n)"})) {
}


string Dialect::sqlOptions() const {
    string opt = "FIELDS TERMINATED BY '" + outTranslationMap.at(fieldsTerminatedBy) + "'";
    if ('\0' != fieldsEnclosedBy) opt += " ENCLOSED BY '" + outTranslationMap.at(fieldsEnclosedBy) + "'";
    opt += " ESCAPED BY '" + outTranslationMap.at(fieldsEscapedBy)  + "'";
    opt += " LINES TERMINATED BY '" + outTranslationMap.at(linesTerminatedBy)  + "'";
    return opt;
}


Parser::Parser(Dialect const& dialect)
    :   _dialect(dialect),
        _lineBuf(new char[MAX_ROW_LENGTH]) {
}


void Parser::parse(char const* inBuf,
                   size_t inBufSize,
                   bool flush,
                   ParsedStringCallbackType const& onStringParsed) {
    string const context = "Csv::Parser::" + string(__func__) + " ";
    if (inBuf == nullptr) throw invalid_argument(context + " input buffer is the null pointer.");

    char const* endBufPtr = inBuf + inBufSize;
    char const* beginLinePtr = inBuf;
    char const* endLinePtr = inBuf;
    do {
        // Keep going if not the line terminator
        if (*endLinePtr++ != _dialect.linesTerminatedBy) continue;
        // Keep going if the line terminator was preceded by the escape character
        size_t const len = endLinePtr - beginLinePtr;
        if (len > 1 && *(endLinePtr - 2) == _dialect.fieldsEscapedBy) continue; 
        // Ignore empty lines. Just count them.
        if (len > 1) {
            if (_lineBufNext == 0) {
                // Report new line directly from the input buffer
                if (len > MAX_ROW_LENGTH) {
                    throw runtime_error(
                            context + " input line " + to_string(_lineNum) + " exceeds the limit of "
                            + to_string(MAX_ROW_LENGTH) + " bytes.");
                }
                onStringParsed(beginLinePtr, len);
            } else {
                // Append the found sub-string to the line buffer and report the complete
                // string from there. Otherwise report the found string from the input buffer.
                if (len > (MAX_ROW_LENGTH - _lineBufNext)) {
                    throw runtime_error(
                            context + " input line " + to_string(_lineNum) + " exceeds the limit of "
                            + to_string(MAX_ROW_LENGTH) + " bytes.");
                }
                memcpy(_lineBuf.get() + _lineBufNext, beginLinePtr, len);
                _lineBufNext += len;
                onStringParsed(_lineBuf.get(), _lineBufNext);
                _lineBufNext = 0;
            }
        }

        // Start a new line
        ++_lineNum;
        beginLinePtr = endLinePtr;

    } while (endLinePtr < endBufPtr);

    // Copy the non-terminated remainder (if any) of the input buffer into
    // into the line buffer.
    size_t const len = endBufPtr - beginLinePtr;
    if (len > 0) {
        if (len > MAX_ROW_LENGTH) {
            throw runtime_error(
                    context + " input line " + to_string(_lineNum) + " exceeds the limit of "
                    + to_string(MAX_ROW_LENGTH) + " bytes.");
        }
        memcpy(_lineBuf.get(), beginLinePtr, len);
        _lineBufNext = len;
    }

    if (flush) {
        // Should have at least 1 character
        if (_lineBufNext > 0) {
            // Insert the line terminator
            if (_lineBufNext >= MAX_ROW_LENGTH) {
                throw runtime_error(
                        context + " input line " + to_string(_lineNum) + " exceeds the limit of "
                        + to_string(MAX_ROW_LENGTH) + " bytes.");
            }
            _lineBuf[_lineBufNext++] = _dialect.linesTerminatedBy;
            onStringParsed(_lineBuf.get(), _lineBufNext);
            _lineBufNext = 0;
            ++_lineNum;
        }
    }
}

}}}} // namespace lsst::qserv::replica::csv
