/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/DatabaseMySQL.h"

// System headers
#include <regex>
#include <sstream>
#include <stdexcept>

// Third party headers
#include <boost/lexical_cast.hpp>
#include <mysql/mysqld_error.h>

// Qserv headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseMySQL");

using Row              = lsst::qserv::replica::database::mysql::Row;
using InvalidTypeError = lsst::qserv::replica::database::mysql::InvalidTypeError;


template <typename K>
bool getAsString(Row const&   row,
                 K            key,
                 std::string& value) {

    Row::Cell const& cell = row.getDataCell(key);
    if (cell.first) {
        value = std::string(cell.first);
        return true;
    }
    return false;
}

template <typename K, class T>
bool getAsNumber(Row const& row,
                 K          key,
                 T&         value) {
    try {
        Row::Cell const& cell = row.getDataCell(key);
        if (cell.first) {
            value = boost::lexical_cast<T>(cell.first, cell.second);
            return true;
        }
        return false;
    } catch (boost::bad_lexical_cast const& ex) {
        throw InvalidTypeError(
                    "DatabaseMySQL::getAsNumber<K,T>()  type conversion failed for key: " + key);
    }
}

}   // namespace

namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {

/////////////////////////////////////////////////////
//                ConnectionParams                 //
/////////////////////////////////////////////////////
#if 0
ConnectionParams ConnectionParams::parse(std::string const& params,
                                         std::string const& defaultHost,
                                         uint16_t           defaultPort,
                                         std::string const& defaultUser,
                                         std::string const& defaultPassword) {

    static std::string const context = "ConnectionParams::parse  ";

    ConnectionParams connectionParams;
    connectionParams.host     = defaultHost;
    connectionParams.port     = defaultPort;
    connectionParams.user     = defaultUser;
    connectionParams.password = defaultPassword;
    connectionParams.database = "";

    std::stringstream is(params);
    std::string token;

    while (std::getline(is, token, ',')) {
        std::string::size_type const pos = token.find('=');
        if ((pos == std::string::npos) or   /* no '=' */
            (pos == 0) or                   /* no parameter name before '=' */
            (pos + 1 >= token.size())) {    /* no value after '=' */
            throw std::invalid_argument(
                    context + "incorrect syntax of the encoded connection parameters string");
        }
        std::string const param = token.substr(0, pos);     /* what's before '=' */
        std::string const val   = token.substr(pos + 1);    /* whats after '=' */

        if      ("host"     == param) { connectionParams.host     = val; }
        else if ("port"     == param) { connectionParams.port     = (uint16_t)std::stoul(val); }
        else if ("user"     == param) { connectionParams.user     = val; }
        else if ("password" == param) { connectionParams.password = val; }
        else if ("database" == param) { connectionParams.database = val; }
        else                          {
            throw std::invalid_argument(
                    context + "unknown parameter found in the encoded parameters string");
        }
    }
    if (connectionParams.database.empty()) {
        throw std::invalid_argument(
                context + "database name not found in the encoded parameters string");
    }
    LOGS(_log, LOG_LVL_DEBUG, context << connectionParams);

    return connectionParams;
}
#else
ConnectionParams ConnectionParams::parse(std::string const& params,
                                         std::string const& defaultHost,
                                         uint16_t           defaultPort,
                                         std::string const& defaultUser,
                                         std::string const& defaultPassword) {

    static std::string const context = "ConnectionParams::parse  ";


    std::regex re("^mysql://([^:]+)?(:([^:]?.*[^@]?))?@([^:^/]+)?(:([0-9]+))?(/([^/]+))?$",
                  std::regex::extended);
    std::smatch match;
    if (not std::regex_search(params, match, re)) {
        throw std::invalid_argument(context + "incorrect syntax of the encoded connection parameters string");
    }
    if (match.size() != 9) {
        throw std::runtime_error(context + "problem with the regular expression");
    }

    ConnectionParams connectionParams;

    std::string const user = match[1].str();
    connectionParams.user  = user.empty() ? defaultUser : user;

    std::string const password = match[3].str();
    connectionParams.password = password.empty() ?  defaultPassword : password;

    std::string const host = match[4].str();
    connectionParams.host  = host.empty() ? defaultHost : host;

    std::string const port = match[6].str();
    connectionParams.port  = port.empty() ?  defaultPort : (uint16_t)std::stoul(port);

    // no default option for the database
    connectionParams.database = match[8].str();
    if (connectionParams.database.empty()) {
        throw std::invalid_argument(
                context + "database name not found in the encoded parameters string");
    }

    LOGS(_log, LOG_LVL_DEBUG, context << connectionParams);


    return connectionParams;
}
#endif
std::string ConnectionParams::toString() const {
    return
        std::string("mysql://") + user + ":xxxxxx@" + host + ":" + std::to_string(port) + "/" + database;
}

std::ostream& operator<<(std::ostream& os, ConnectionParams const& params) {
    os  << "DatabaseMySQL::ConnectionParams " << "(" << params.toString() << ")";
    return os;
}

///////////////////////////////////////
//                Row                //
///////////////////////////////////////

Row::Row()
    :   _name2indexPtr(nullptr) {
}

size_t Row::numColumns() const {
    if (not isValid()) {
        throw std::logic_error("Row::numColumns()  the object is not valid");
    }
    return  _index2cell.size();
}

bool Row::isNull(size_t              columnIdx) const { return not getDataCell(columnIdx) .first; }
bool Row::isNull(std::string const& columnName) const { return not getDataCell(columnName).first; }

bool Row::get(size_t             columnIdx,  std::string& value) const { return ::getAsString(*this, columnIdx,  value); }
bool Row::get(std::string const& columnName, std::string& value) const { return ::getAsString(*this, columnName, value); }

bool Row::get(size_t columnIdx, uint64_t& value) const { return ::getAsNumber(*this, columnIdx, value); }
bool Row::get(size_t columnIdx, uint32_t& value) const { return ::getAsNumber(*this, columnIdx, value); }
bool Row::get(size_t columnIdx, uint16_t& value) const { return ::getAsNumber(*this, columnIdx, value); }
bool Row::get(size_t columnIdx, uint8_t&  value) const { return ::getAsNumber(*this, columnIdx, value); }

bool Row::get(std::string const& columnName, uint64_t& value) const { return ::getAsNumber(*this, columnName, value); }
bool Row::get(std::string const& columnName, uint32_t& value) const { return ::getAsNumber(*this, columnName, value); }
bool Row::get(std::string const& columnName, uint16_t& value) const { return ::getAsNumber(*this, columnName, value); }
bool Row::get(std::string const& columnName, uint8_t&  value) const { return ::getAsNumber(*this, columnName, value); }

bool Row::get(size_t columnIdx, int64_t& value) const { return ::getAsNumber(*this, columnIdx, value); }
bool Row::get(size_t columnIdx, int32_t& value) const { return ::getAsNumber(*this, columnIdx, value); }
bool Row::get(size_t columnIdx, int16_t& value) const { return ::getAsNumber(*this, columnIdx, value); }
bool Row::get(size_t columnIdx, int8_t&  value) const { return ::getAsNumber(*this, columnIdx, value); }

bool Row::get(std::string const& columnName, int64_t& value) const { return ::getAsNumber(*this, columnName, value); }
bool Row::get(std::string const& columnName, int32_t& value) const { return ::getAsNumber(*this, columnName, value); }
bool Row::get(std::string const& columnName, int16_t& value) const { return ::getAsNumber(*this, columnName, value); }
bool Row::get(std::string const& columnName, int8_t&  value) const { return ::getAsNumber(*this, columnName, value); }

bool Row::get(size_t columnIdx, float&  value) const { return ::getAsNumber(*this, columnIdx, value); }
bool Row::get(size_t columnIdx, double& value) const { return ::getAsNumber(*this, columnIdx, value); }

bool Row::get(std::string const& columnName, float&  value) const { return ::getAsNumber(*this, columnName, value); }
bool Row::get(std::string const& columnName, double& value) const { return ::getAsNumber(*this, columnName, value); }

bool Row::get(size_t columnIdx, bool& value) const {
    uint8_t number;
    if (::getAsNumber(*this, columnIdx, number)) {
        value = (bool) number;
        return true;
    }
    return false;
}

bool Row::get(std::string const& columnName, bool&  value) const {
    uint8_t number;
    if (::getAsNumber(*this, columnName, number)) {
        value = (bool) number;
        return true;
    }
    return false;
}

Row::Cell const& Row::getDataCell(size_t columnIdx) const {

    static std::string const context = "Row::getDataCell()  ";

    if (not isValid()) {
        throw std::logic_error(context + "the object is not valid");
    }
    if (columnIdx >= _index2cell.size()) {
        throw std::invalid_argument(
                context + "the column index '" + std::to_string(columnIdx) +
                "'is not in the result set");
    }
    return _index2cell.at(columnIdx);
}

Row::Cell const& Row::getDataCell(std::string const& columnName) const {

    static std::string const context = "Row::getDataCell()  ";

    if (not isValid()) {
        throw std::logic_error(context + "the object is not valid");
    }
    if (not _name2indexPtr->count(columnName)) {
        throw std::invalid_argument(
                context + "the column '" + columnName + "'is not in the result set");
    }
    return _index2cell.at(_name2indexPtr->at(columnName));
}

//////////////////////////////////////////////////
//                DoNotProcess                  //
//////////////////////////////////////////////////

DoNotProcess::DoNotProcess(std::string const& name_)
    :   name(name_) {
}

/////////////////////////////////////////////
//                Keyword                  //
/////////////////////////////////////////////

Keyword const Keyword::SQL_NULL {"NULL"};

Keyword::Keyword(std::string const& name_)
    :   DoNotProcess(name_) {
}

/////////////////////////////////////////////
//                Function                 //
/////////////////////////////////////////////

Function const Function::LAST_INSERT_ID {"LAST_INSERT_ID()"};

Function::Function(std::string const& name_)
    :   DoNotProcess(name_) {
}

///////////////////////////////////////////////
//                Connection                 //
///////////////////////////////////////////////

Connection::Ptr Connection::open(ConnectionParams const& connectionParams,
                                     bool autoReconnect,
                                     bool autoCommit) {

    Connection::Ptr ptr(new Connection(connectionParams,
                                           autoReconnect,
                                           autoCommit));
    ptr->connect();
    return ptr;
}

Connection::Connection(ConnectionParams const& connectionParams,
                       bool autoReconnect,
                       bool autoCommit)
    :   _connectionParams(connectionParams),
        _autoReconnect(autoReconnect),
        _autoCommit(autoCommit),
        _inTransaction(false),
        _mysql(nullptr),
        _res(nullptr),
        _fields(nullptr),
        _numFields(0) {
}


Connection::~Connection() {
    if (_res) {
        mysql_free_result(_res);
        mysql_close(_mysql);
    }
}

std::string Connection::escape(std::string const& inStr) const {

    static std::string const context = "Connection::escape()  ";

    if (not _mysql) {
        throw Error(context + "not connected to the MySQL service");
    }
    size_t const inLen = inStr.length();

    // Allocate at least that number of bytes to cover the worst case scenario
    // of each input character to be escaped plus the end of string terminator.
    // See: https://dev.mysql.com/doc/refman/5.7/en/mysql-real-escape-string.html

    size_t const outLenMax = 2*inLen + 1;

    // The temporary storage will get automatically deconstructed in the end
    // of this block.

    std::unique_ptr<char[]> outStr(new char[outLenMax]);
    size_t const outLen =
        mysql_real_escape_string(
            _mysql,
            outStr.get(),
            inStr.c_str(),
            inLen);

    return std::string(outStr.get(), outLen) ;
}

Connection::Ptr Connection::begin() {
    assertTransaction(false);
    execute("BEGIN");
    _inTransaction = true;
    return shared_from_this();
}


Connection::Ptr Connection::commit() {
    assertTransaction(true);
    execute("COMMIT");
    _inTransaction = false;
    return shared_from_this();
}

Connection::Ptr Connection::rollback() {
    assertTransaction(true);
    execute("ROLLBACK");
    _inTransaction = false;
    return shared_from_this();
}

Connection::Ptr Connection::execute(std::string const& query) {

    static std::string const context = "Connection::execute()  ";

    LOGS(_log, LOG_LVL_DEBUG, context << query);

    if (query.empty()) {
        throw std::invalid_argument(
                context + "empty query string passed into the object");
    }

    // Reset/initialize the query context before attempting to execute
    // the new  query.

    _lastQuery = query;

    if (_res) mysql_free_result (_res) ;
    _res       = nullptr;
    _fields    = nullptr;
    _numFields = 0;

    _columnNames.clear();
    _name2index.clear();

    if (mysql_real_query(_mysql,
                         _lastQuery.c_str(),
                         _lastQuery.size())) {

        std::string const msg =
            context + "query: '" + _lastQuery + "', error: " +
            std::string(mysql_error(_mysql));

        switch (mysql_errno(_mysql)) {
            case ER_DUP_ENTRY: throw DuplicateKeyError(msg);
            default:           throw Error(msg);
        }
    }

    // Fetch result set for queries which return the one

    if (mysql_field_count(_mysql)) {

        // unbuffered read
        _res = mysql_use_result(_mysql);
        if (not _res) {
            throw Error(context + "mysql_use_result failed, error: " +
                        std::string(mysql_error(_mysql)));
        }
        _fields    = mysql_fetch_fields(_res);
        _numFields = mysql_num_fields(_res);

        for (size_t i = 0; i < _numFields; i++) {
            _columnNames.push_back(std::string(_fields[i].name));
            _name2index[_fields[i].name] = i;
        }
    }
    return shared_from_this();
}

bool Connection::hasResult() const {
    return _mysql and _res;
}

std::vector<std::string> const& Connection::columnNames() const {
    assertQueryContext();
    return _columnNames;
}

bool Connection::next(Row& row) {

    static std::string const context = "Connection::next()  ";

    assertQueryContext();

    _row = mysql_fetch_row(_res);
    if (not _row) {

        // Just no more rows is no specific error reported
        if (not mysql_errno(_mysql)) { return false; }

        throw Error(context + "mysql_fetch_row failed, error: " +
                    std::string(mysql_error(_mysql)) +
                    ", query: '" + _lastQuery + "'");
    }
    size_t const* lengths = mysql_fetch_lengths(_res);

    // Transfer the data pointers for each field and their lengths into
    // the provided Row object.

    row._name2indexPtr = &_name2index;
    row._index2cell.clear();

    row._index2cell.reserve(_numFields);
    for (size_t i = 0; i < _numFields; ++i) {
        row._index2cell.emplace_back(Row::Cell{_row[i], lengths[i]});
    }
    return true;
}

void Connection::connect() {

    static std::string const context = "Connection::connect()  ";

    // Prepare the connection object
    if (not (_mysql = mysql_init(_mysql))) {
        throw Error(context + "mysql_init failed");
    }

    // Allow automatic reconnect if requested
    if (_autoReconnect) {
        my_bool reconnect = 1;
        mysql_options(_mysql, MYSQL_OPT_RECONNECT, &reconnect);
    }

    // Connect now
    if (not mysql_real_connect(
        _mysql,
        _connectionParams.host.empty()     ? nullptr : _connectionParams.host.c_str(),
        _connectionParams.user.empty()     ? nullptr : _connectionParams.user.c_str(),
        _connectionParams.password.empty() ? nullptr : _connectionParams.password.c_str(),
        _connectionParams.database.empty() ? nullptr : _connectionParams.database.c_str(),
        _connectionParams.port,
        0,  /* no default UNIX socket */
        0)) /* no default client flag */
        throw Error(context + "mysql_real_connect() failed, error: " +
                    std::string(mysql_error(_mysql)));

    // Set session attributes
    if (mysql_query(_mysql, "SET SESSION SQL_MODE='ANSI'") or
        mysql_query(_mysql, _autoCommit ? "SET SESSION AUTOCOMMIT=1" :
                                          "SET SESSION AUTOCOMMIT=0")) {
        throw Error(context + "mysql_query() failed, error: " +
                    std::string(mysql_error(_mysql)));
    }
}

void Connection::assertQueryContext() const {

    static std::string const context = "Connection::assertQueryContext()  ";

    if (not _mysql) { throw Error(context + "not connected to the MySQL service"); }
    if (not _res)   { throw Error(context + "no prior query made"); }
}

void Connection::assertTransaction(bool inTransaction) const {

    static std::string const context = "Connection::assertTransaction()  ";

    if (inTransaction != _inTransaction) {
        throw std::logic_error(
                context + "the transaction is" +
                std::string( _inTransaction ? " " : " not") + " active");
    }
}

}}}}} // namespace lsst::qserv::replica::database::mysql
