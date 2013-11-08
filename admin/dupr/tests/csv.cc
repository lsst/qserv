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

#include <stdexcept>
#include <string>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Csv
#include "boost/test/unit_test.hpp"

#include "Csv.h"

namespace csv = lsst::qserv::admin::dupr::csv;

using std::runtime_error;
using std::string;
using std::vector;

using csv::Dialect;
using csv::Editor;

BOOST_AUTO_TEST_CASE(DialectTest) {
    char const X[2] = { '\n', '\r' };
    BOOST_CHECK_THROW(Dialect('\0', '\\', '"'), runtime_error);
    for (size_t i = 0; i < 2; ++i) {
        BOOST_CHECK_THROW(Dialect(X[i], '\\',  '"'), runtime_error);
        BOOST_CHECK_THROW(Dialect(',',  X[i],  '"'), runtime_error);
        BOOST_CHECK_THROW(Dialect(',',  '\\', X[i]), runtime_error);
    }
    BOOST_CHECK_THROW(Dialect(',', ',', '"'), runtime_error);
    BOOST_CHECK_THROW(Dialect(',', '\\', ','), runtime_error);
    BOOST_CHECK_THROW(Dialect(',', '\\', '\\'), runtime_error);
    string x("0bfnrtvNZ");
    for (size_t i = 0; i < x.size(); ++i) {
        BOOST_CHECK_THROW(Dialect(x[i], '\\',  '"'), runtime_error);
        BOOST_CHECK_THROW(Dialect(',',  x[i],  '"'), runtime_error);
        BOOST_CHECK_THROW(Dialect(',',  '\\', x[i]), runtime_error);
    }
    BOOST_CHECK_THROW(Dialect("nil\n", '|', '\\', '"'), runtime_error);
    BOOST_CHECK_THROW(Dialect("nil\r", '|', '\\', '"'), runtime_error);
    BOOST_CHECK_THROW(Dialect("nil|",  '|', '\\', '"'), runtime_error);
}

BOOST_AUTO_TEST_CASE(CodingTest) {
    Dialect d("None", '|', '\\', '\"');
    string s = d.encode("\0", 1);
    BOOST_CHECK(d.decode(s.data(), s.size()).compare(0, string::npos, "\0", 1) == 0);
    s = d.encode("\1", 1);
    BOOST_CHECK(d.decode(s.data(), s.size()).compare(0, string::npos, "\1", 1) == 0);
    s = d.encode("None", 4);
    BOOST_CHECK_EQUAL(s, "\"None\"");
    BOOST_CHECK_EQUAL(d.decode(s.data(), s.size()), "None");
    BOOST_CHECK_EQUAL(d.encode(0,0), "None");
    s = d.encode("|\\\"", 3);
    BOOST_CHECK_EQUAL(s, "\\|\\\\\\\"");
    s = d.encode("foo", 3);
    BOOST_CHECK_EQUAL(d.encode("foo", 3), "foo");
    BOOST_CHECK_EQUAL(d.decode("foo", 3), "foo");
    BOOST_CHECK_EQUAL(d.decode("a\"b",3), "a\"b");
    BOOST_CHECK_EQUAL(d.decode("a\"\"", 3), "a\"\"");
    BOOST_CHECK_EQUAL(d.decode("a\"\"b",4), "a\"\"b");
    BOOST_CHECK_EQUAL(d.decode("\"a\"\"b\"",6), "a\"b");
    BOOST_CHECK_EQUAL(d.decode("\"a",2), "a");
    BOOST_CHECK_EQUAL(d.decode("\"\"a",3), "\"a");
}

BOOST_AUTO_TEST_CASE(CodingNoEscapeTest) {
    Dialect d('|', '\0', '\'');
    string s = d.encode("\0", 1);
    BOOST_CHECK(d.decode(s.data(), s.size()).compare(0, string::npos, "\0", 1) == 0);
    s = d.encode("\1", 1);
    BOOST_CHECK(d.decode(s.data(), s.size()).compare(0, string::npos, "\1", 1) == 0);
    s = d.encode("NULL", 4);
    BOOST_CHECK_EQUAL(s, "'NULL'");
    BOOST_CHECK_EQUAL(d.decode(s.data(), s.size()), "NULL");
    BOOST_CHECK_EQUAL(d.encode(0, 0), "NULL");
    BOOST_CHECK_THROW(d.encode("\n",1), runtime_error);
    BOOST_CHECK_THROW(d.encode("\r",1), runtime_error);
    BOOST_CHECK_EQUAL(d.encode("|",1), "'|'");
    BOOST_CHECK_EQUAL(d.decode("'|'", 3), "|");
    BOOST_CHECK_EQUAL(d.encode("'",1), "''''");
    BOOST_CHECK_EQUAL(d.decode("a'b",3), "a'b");
    BOOST_CHECK_EQUAL(d.decode("a''", 3), "a''");
    BOOST_CHECK_EQUAL(d.decode("a''b",4), "a''b");
    BOOST_CHECK_EQUAL(d.decode("'a''b'",6), "a'b");
    BOOST_CHECK_EQUAL(d.decode("'a",2), "a");
    BOOST_CHECK_EQUAL(d.decode("''a",3), "'a");
}

BOOST_AUTO_TEST_CASE(CodingNoQuoteTest) {
    Dialect d(',', '/', '\0');
    string s = d.encode("\0", 1);
    BOOST_CHECK(d.decode(s.data(), s.size()).compare(0, string::npos, "\0", 1) == 0);
    s = d.encode("\1", 1);
    BOOST_CHECK(d.decode(s.data(), s.size()).compare(0, string::npos, "\1", 1) == 0);
    s = d.encode("/N", 2);
    BOOST_CHECK_EQUAL(s, "//N");
    BOOST_CHECK_EQUAL(d.decode(s.data(), s.size()), "/N");
    BOOST_CHECK_EQUAL(d.encode(0, 0), "/N");
    BOOST_CHECK_EQUAL(d.encode("\n\r\b\t\v",5), "/n/r\b\t\v");
    BOOST_CHECK_EQUAL(d.decode("/n/r/b/t/v",10), "\n\r\b\t\v");
    BOOST_CHECK_EQUAL(d.encode(",",1), "/,");
    BOOST_CHECK_EQUAL(d.decode("/,",2), ",");
}

BOOST_AUTO_TEST_CASE(EditorTest) {
    Dialect d('|', '\\', '\0');
    vector<string> inames, onames;
    inames.push_back("foo");
    inames.push_back("foo");
    BOOST_CHECK_THROW(Editor(d, d, inames, onames), runtime_error);
    inames.pop_back();
    inames.push_back("bar");
    Editor ed(d, d, inames, onames);
    BOOST_CHECK_EQUAL(ed.getNumInputFields(), 2);
    BOOST_CHECK_EQUAL(ed.getFieldIndex("foo"), 0);
    BOOST_CHECK_EQUAL(ed.getFieldIndex("bar"), 1);
    BOOST_CHECK(ed.isInputField("foo"));
    BOOST_CHECK(ed.isInputField("bar"));
    BOOST_CHECK(ed.isNull("foo") && ed.isNull("bar"));
    BOOST_CHECK(ed.isNull(0) && ed.isNull(1));
    BOOST_CHECK_EQUAL(ed.get(0, false), "\\N");
    BOOST_CHECK_THROW(ed.get(0, true), runtime_error);
    BOOST_CHECK_THROW(ed.get(-1, false), runtime_error);
    BOOST_CHECK_THROW(ed.get(2, false), runtime_error);
    BOOST_CHECK_THROW(ed.get("baz", false), runtime_error);
    BOOST_CHECK_THROW(ed.get<char>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<unsigned char>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<signed char>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<short>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<unsigned short>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<int>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<unsigned int>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<long>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<unsigned long>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<long long>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<unsigned long long>(1), runtime_error);
    BOOST_CHECK_THROW(ed.get<float>(0), runtime_error);
    BOOST_CHECK_THROW(ed.get<double>(0), runtime_error);
    char const * x[6] = {
         "foo",
         "foo\n",
         "foo\r",
         "foo|bar|baz",
         "",
         "foo|bar\\"
    };
    for (int i = 0; i < 6; ++i) {
        BOOST_CHECK_THROW(ed.readRecord(x[i], x[i] + strlen(x[i])), runtime_error);
    }
    char const * r = "10000|3.1415926\r\n";
    char const * e = r + strlen(r);
    BOOST_CHECK(ed.readRecord(r, e) == e);
    BOOST_CHECK_THROW(ed.get<char>(0), runtime_error);
    BOOST_CHECK_THROW(ed.get<unsigned char>(0), runtime_error);
    BOOST_CHECK_THROW(ed.get<signed char>(0), runtime_error);
    BOOST_CHECK_THROW(ed.get<int>("bar"), runtime_error);
    BOOST_CHECK_THROW(ed.get<long>(1), runtime_error);
    BOOST_CHECK_EQUAL(ed.get<short>("foo"), 10000);
    BOOST_CHECK_EQUAL(ed.get<unsigned>("foo"), 10000u);
    BOOST_CHECK_EQUAL(ed.get<long long>(0), 10000);
    BOOST_CHECK_EQUAL(ed.get<float>(1), 3.1415926f);
    BOOST_CHECK_EQUAL(ed.get<double>("bar"), 3.1415926);
    char buf[8];
    buf[0] = '\0';
    BOOST_CHECK(ed.writeRecord(buf) == buf + 1);
    BOOST_CHECK_EQUAL(buf[0], '\n');
}

BOOST_AUTO_TEST_CASE(EditorTranscodeTest) {
    char buf[MAX_LINE_SIZE + 1];
    Dialect in('|', '/', '\0');
    Dialect out("nil", ',', '\\', '\'');
    vector<string> inames, onames;
    inames.push_back("a");
    inames.push_back("b");
    inames.push_back("c");
    onames.push_back("c");
    onames.push_back("a");
    onames.push_back("c");
    onames.push_back("d");
    Editor ed(in, out, inames, onames);
    char const * s = "a|b,|/N";
    char const * se = s + strlen(s);
    BOOST_CHECK(ed.readRecord(s, se) == se);
    BOOST_CHECK_EQUAL(ed.get<string>("a"), "a");
    BOOST_CHECK_EQUAL(ed.get<string>("b"), "b,");
    BOOST_CHECK(ed.isNull("c"));
    ed.set(ed.getFieldIndex("d"), 5);
    char * be = ed.writeRecord(buf);
    *be = '\0';
    BOOST_CHECK_EQUAL(string("nil,a,nil,5\n"), buf);
}
