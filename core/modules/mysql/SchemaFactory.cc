// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#include "mysql/SchemaFactory.h"

// System headers
#include <sstream>

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
#include "sql/Schema.h"

namespace lsst {
namespace qserv {
namespace mysql {

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

struct ColTypeFactory {
    /// Construct a ColTypeFactory tied to a ColType. Each invocation of
    /// buildTo() fills the associated ColType.
    ColTypeFactory(sql::ColType& ct)
        : mysqlType(ct.mysqlType),
          sqlType(ct.sqlType) {}

    /// Set the attached ColType according to an input MYSQL_FIELD
    void buildTo(MYSQL_FIELD const& f) {
        mysqlType = f.type;
        switch(f.type) {
        case MYSQL_TYPE_DECIMAL: _setDecimal(f); break;
        case MYSQL_TYPE_TINY: _setGeneric("TINYINT", f.length); break;//,n
        case MYSQL_TYPE_SHORT: _setGeneric("SMALLINT", f.length); break;//,n
        case MYSQL_TYPE_LONG: _setGeneric("INT", f.length); break; //,n
        case MYSQL_TYPE_FLOAT: sqlType = "FLOAT"; // n,m
        case MYSQL_TYPE_DOUBLE: sqlType = "DOUBLE"; break; // n,m
        case MYSQL_TYPE_NULL: sqlType = "NULL"; break;

        case MYSQL_TYPE_TIMESTAMP: sqlType = "TIMESTAMP"; break;
        case MYSQL_TYPE_LONGLONG: _setGeneric("BIGINT", f.length); break;
        case MYSQL_TYPE_INT24: sqlType = "INT24??"; break;
        case MYSQL_TYPE_DATE: sqlType = "DATE"; break;
        case MYSQL_TYPE_TIME: sqlType = "TIME"; break;
        case MYSQL_TYPE_DATETIME: sqlType = "DATETIME"; break;
        case MYSQL_TYPE_YEAR: sqlType = "YEAR"; break;
        case MYSQL_TYPE_NEWDATE: sqlType = "DATE"; break;
        case MYSQL_TYPE_VARCHAR: sqlType = "VARCHAR"; break; // n
        case MYSQL_TYPE_BIT: sqlType = "BIT"; // length landling
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_ENUM: sqlType = "ENUM??"; break; // flag handling
        case MYSQL_TYPE_SET: sqlType = "SET??"; break; // flag handling
        case MYSQL_TYPE_TINY_BLOB: _setBlobOrText("TINY", f); break;
        case MYSQL_TYPE_MEDIUM_BLOB: _setBlobOrText("MEDIUM", f); break;
        case MYSQL_TYPE_LONG_BLOB: _setBlobOrText("LONG", f); break;
        case MYSQL_TYPE_BLOB: _setBlobOrText("", f); break;
        case MYSQL_TYPE_VAR_STRING: _setVarString(f); break;
        case MYSQL_TYPE_STRING: _setString(f); break;
        case MYSQL_TYPE_GEOMETRY: sqlType = "GEOM??"; break; // point, linestring, etc.
        default:
            break;
        }
    }

private:
    inline bool _hasCharset(MYSQL_FIELD const& f) {
        // charset: https://dev.mysql.com/doc/refman/5.0/en/c-api-data-structures.html
        return (f.charsetnr != 63);  // 63 -> binary
    }

    inline bool _hasFlagUnsigned(MYSQL_FIELD const& f) {
        return f.flags & UNSIGNED_FLAG;
    }


    inline void _setBlobOrText(char const* variant, MYSQL_FIELD const& f) {
        std::ostringstream os;
        os <<  variant << (_hasCharset(f) ? "TEXT" : "BLOB");
        sqlType = os.str();
    }

    void _setGeneric(char const* baseType, int length) {
        std::ostringstream os;
        os <<  baseType << "(" << length << ")";
        sqlType = os.str();
    }

    void _setDecimal(MYSQL_FIELD const& f) {
        // See mysql src sql/field.cc:Field_decimal::sql_type()
        unsigned int tmp = f.length;
        if(_hasFlagUnsigned(f)) --tmp;
        if(f.decimals > 0) --tmp;
        std::ostringstream os;
        os << "DECIMAL(" << tmp << "," <<  f.decimals << ")";
        sqlType = os.str();
    }

    void _setString(MYSQL_FIELD const& f) {
        // See mysql src sql/field.cc:Field_string::sql_type()
        std::ostringstream os;
        os << (_hasCharset(f) ? "CHAR(" : "BINARY(" )
           << f.length; // Cheat and skip the actual charset handling.
        // For charsets with non single-byte characters, this
        // overestimates the width, which is fine for us.
        os << ")";
        sqlType = os.str();
    }

    void _setVarString(MYSQL_FIELD const& f) {
        // See mysql src sql/field.cc:Field_varstring::sql_type(String &res) const
        std::ostringstream os;
        os << (_hasCharset(f) ? "VARCHAR(" : "VARBINARY(")
           << f.length; // Cheat and skip the actual charset handling.
        // For charsets with non single-byte characters, this
        // overestimates the width, which is fine for us.
        os << ")";
        sqlType = os.str();
    }

    // References into the ColType passed during construction.
    int& mysqlType;
    std::string& sqlType;
};

/// Set a ColSchema according to the contents of a MYSQL_FIELD
void setColSchemaTo(sql::ColSchema& cs, MYSQL_FIELD const& f) {
    cs.name = f.name;
    cs.hasDefault = false;
    if(f.def_length) {
        // If there is a default value stored, record it.
        // There is probably a default value.
        cs.defaultValue = std::string(f.def, f.def_length);
        cs.hasDefault = true;
        // (I hope I don't need to escape this string--if I do, then I
        // need a MYSQL*, which is ridiculous, but
        // mysql_real_escape_string needs one so it can peek inside
        // for the charset, rather than allowing you to specify a
        // charset externally. -danielw )
    }
    // ...but BLOBs can't have default values
    switch(f.type) {
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
        cs.hasDefault = false;
    default:
        break;
    }
    // ...and if the flag is set, then you really can't have a default value.
    if(f.flags & NO_DEFAULT_VALUE_FLAG) {
        cs.hasDefault = false;
    }
}

////////////////////////////////////////////////////////////////////////
// SchemaFactory implementation
////////////////////////////////////////////////////////////////////////

/// Construct a ColType from a MYSQL_FIELD
sql::ColType SchemaFactory::newColType(MYSQL_FIELD const& f) {
    sql::ColType ct;
    ColTypeFactory ctf(ct);
    ctf.buildTo(f);
    return ct;
}

/// Construct a ColSchema from a valid MYSQL_FIELD
sql::ColSchema SchemaFactory::newColSchema(MYSQL_FIELD const& f) {
    sql::ColSchema cs;
    ColTypeFactory ctf(cs.colType);
    ctf.buildTo(f);
    setColSchemaTo(cs, f);
    return cs;
}

/// Construct a Schema from a result.
/// May not be called except after mysql_store_result or mysql_use_result
sql::Schema SchemaFactory::newFromResult(MYSQL_RES* result) {
    sql::Schema s;
    MYSQL_FIELD* field;
    while((field = mysql_fetch_field(result))) {
        s.columns.push_back(newColSchema(*field));
    }
    return s;
}

}}} // namespace lsst::qserv::mysql
