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
#include <cstring>
#include <map>

using namespace std;
using json = nlohmann::json;

namespace {

string const context = "Csv::Parser: ";

map<string, char> const inTranslationMap = {{R"(\0)", '\0'}, {R"(\t)", '\t'}, {R"(,)", ','},  {R"(')", '\''},
                                            {R"(")", '"'},   {R"(\\)", '\\'}, {R"(\n)", '\n'}};

map<char, string> const outTranslationMap = {{'\0', R"()"}, {'\t', R"(\t)"}, {',', R"(,)"},  {'\'', R"(\')"},
                                             {'"', R"(")"}, {'\\', R"(\\)"}, {'\n', R"(\n)"}};

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
char parseParam(string const& name, string const& value, vector<string> const& allowedValues) {
    if (allowedValues.empty()) {
        throw logic_error(context + "calling the method with empty choices to translate the value '" + value +
                          "' of the parameter '" + name + "'.");
    }
    if (value.empty()) {
        throw invalid_argument(context + "a value of the parameter '" + name + "' is empty.");
    }
    if (allowedValues.cend() == find(allowedValues.cbegin(), allowedValues.cend(), value)) {
        throw invalid_argument(context + "the value '" + value + "' of the parameter '" + name +
                               "' is not allowed for the given parameter.");
    }
    auto const itr = inTranslationMap.find(value);
    if (inTranslationMap.cend() == itr) {
        throw invalid_argument(context + "the value '" + value + "' of the parameter '" + name +
                               "' is not supported by the Parser.");
    }
    return itr->second;
}
}  // namespace

namespace lsst { namespace qserv { namespace replica { namespace csv {

string const Dialect::defaultFieldsTerminatedBy = R"(\t)";
string const Dialect::defaultFieldsEnclosedBy =
        R"(\0)";  // The special value to indicate a lack of enclosing characters
string const Dialect::defaultFieldsEscapedBy = R"(\\)";
string const Dialect::defaultLinesTerminatedBy = R"(\n)";

vector<string> const Dialect::allowedFieldsTerminatedBy = {R"(\t)", R"(,)"};
vector<string> const Dialect::allowedFieldsEnclosedBy = {R"(\0)", R"(')", R"(")"};
vector<string> const Dialect::allowedFieldsEscapedBy = {R"(\\)"};
vector<string> const Dialect::allowedLinesTerminatedBy = {R"(\n)"};

Dialect::Dialect()
        : _fieldsTerminatedBy('\t'),
          _fieldsEnclosedBy('\0'),
          _fieldsEscapedBy('\\'),
          _linesTerminatedBy('\n') {}

Dialect::Dialect(DialectInput const& dialectInput)
        : _fieldsTerminatedBy(::parseParam("fieldsTerminatedBy", dialectInput.fieldsTerminatedBy,
                                           allowedFieldsTerminatedBy)),
          _fieldsEnclosedBy(
                  ::parseParam("fieldsEnclosedBy", dialectInput.fieldsEnclosedBy, allowedFieldsEnclosedBy)),
          _fieldsEscapedBy(
                  ::parseParam("fieldsEscapedBy", dialectInput.fieldsEscapedBy, allowedFieldsEscapedBy)),
          _linesTerminatedBy(::parseParam("linesTerminatedBy", dialectInput.linesTerminatedBy,
                                          allowedLinesTerminatedBy)) {}

string Dialect::sqlOptions() const {
    string opt = "FIELDS TERMINATED BY '" + ::outTranslationMap.at(_fieldsTerminatedBy) + "'";
    if ('\0' != _fieldsEnclosedBy) opt += " ENCLOSED BY '" + ::outTranslationMap.at(_fieldsEnclosedBy) + "'";
    opt += " ESCAPED BY '" + ::outTranslationMap.at(_fieldsEscapedBy) + "'";
    opt += " LINES TERMINATED BY '" + ::outTranslationMap.at(_linesTerminatedBy) + "'";
    return opt;
}

Parser::Parser(Dialect const& dialect) : _dialect(dialect), _lineBuf(new char[MAX_ROW_LENGTH]) {}

DialectInput::DialectInput(ProtocolDialectInput const& obj)
        : fieldsTerminatedBy(obj.fields_terminated_by()),
          fieldsEnclosedBy(obj.fields_enclosed_by()),
          fieldsEscapedBy(obj.fields_escaped_by()),
          linesTerminatedBy(obj.lines_terminated_by()) {}

unique_ptr<ProtocolDialectInput> DialectInput::toProto() const {
    unique_ptr<ProtocolDialectInput> ptr(new ProtocolDialectInput());
    ptr->set_fields_terminated_by(fieldsTerminatedBy);
    ptr->set_fields_enclosed_by(fieldsEnclosedBy);
    ptr->set_fields_escaped_by(fieldsEscapedBy);
    ptr->set_lines_terminated_by(linesTerminatedBy);
    return ptr;
}

json DialectInput::toJson() const {
    return json({{"fields_terminated_by", fieldsTerminatedBy},
                 {"fields_enclosed_by", fieldsEnclosedBy},
                 {"fields_escaped_by", fieldsEscapedBy},
                 {"lines_terminated_by", linesTerminatedBy}});
}

void Parser::parse(char const* inBuf, size_t inBufSize, bool flush,
                   ParsedStringCallbackType const& onStringParsed) {
    if (inBuf == nullptr) throw invalid_argument(context + "input buffer is the null pointer.");

    char const* endBufPtr = inBuf + inBufSize;
    for (char const* ptr = inBuf; ptr != endBufPtr; ++ptr) {
        if (_lineBufNext == MAX_ROW_LENGTH) {
            throw runtime_error(context + "input line " + to_string(_lineNum) + " exceeds the limit of " +
                                to_string(MAX_ROW_LENGTH) + " bytes.");
        }
        char const ch = *ptr;
        _lineBuf[_lineBufNext++] = ch;
        if (ch == _dialect.fieldsEscapedBy()) {
            // Two subsequent escapes eliminate each other.
            _inEscapeMode = !_inEscapeMode;
        } else if (ch == _dialect.linesTerminatedBy() && !_inEscapeMode) {
            onStringParsed(_lineBuf.get(), _lineBufNext);
            _lineBufNext = 0;
            _lineNum++;
        } else {
            // Escape (if any) has been applied to the current character.
            _inEscapeMode = false;
        }
    }
    if (flush && (_lineBufNext != 0)) {
        onStringParsed(_lineBuf.get(), _lineBufNext);
        _lineBufNext = 0;
        _lineNum++;
        _inEscapeMode = false;
    }
}

}}}}  // namespace lsst::qserv::replica::csv
