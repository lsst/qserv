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
#ifndef LSST_QSERV_REPLICA_DATABASEMYSQLTYPES_H
#define LSST_QSERV_REPLICA_DATABASEMYSQLTYPES_H

// System headers
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>

// Qserv headers
#include "replica/FileUtils.h"

// This header declarations
namespace lsst::qserv::replica::database::mysql {
/**
 * Class ConnectionParams encapsulates connection parameters to
 * a MySQL server. If constructed using the default constructor
 * the parameters will be initialized with some reasonable defaults:
 *
 *   host: localhost
 *   port: 3306
 *   user: effective user id of a process
 *
 * The following parameters will be empty:
 *
 *   password:
 *   database:
 */
class ConnectionParams {
public:
    /**
     * The factory method will return an instance of this structure initialized
     * by values of parameters found in the input encoded string. The string is
     * expected to have the following syntax:
     * @code
     *   mysql://[user][:password]@[host][:port]/database
     * @code
     *
     * @note
     *   1) all (but the database) keywords are mandatory
     *   2) default values for other parameters (if missing in the string) will be assumed.
     *
     * @param params connection parameters packed into a string
     * @param defaultHost default value for a host name
     * @param defaultPort default port number
     * @param defaultUser default value for a database user account
     * @param defaultPassword default value for a database user account
     * @throw std::invalid_argument if the string can't be parsed
     */
    static ConnectionParams parse(std::string const& params, std::string const& defaultHost = "localhost",
                                  uint16_t defaultPort = 3306,
                                  std::string const& defaultUser = FileUtils::getEffectiveUser(),
                                  std::string const& defaultPassword = std::string());

    /// Initialize connection parameters with default values
    ConnectionParams();

    /// Normal constructor
    ConnectionParams(std::string const& host_, uint16_t port_, std::string const& user_,
                     std::string const& password_, std::string const& database_);

    ConnectionParams(ConnectionParams const&) = default;
    ConnectionParams& operator=(ConnectionParams const&) = default;

    ~ConnectionParams() = default;

    bool operator==(ConnectionParams const& rhs) const;
    bool operator!=(ConnectionParams const& rhs) const { return not operator==(rhs); };

    /**
     * @param showPassword if 'false' then hash a password in the result
     * @return a string representation of all (but the password unless requested)
     *   parameters. The result will be formatted similarly to the one expected by
     *   the non-default constructor of the class.
     */
    std::string toString(bool showPassword = false) const;

    std::string host;      ///< The DNS name or IP address of a machine where the MySQL service runs
    uint16_t port;         ///< The port number of the MySQL service
    std::string user;      ///< The name of a database user
    std::string password;  ///< The database password
    std::string database;  ///< The name of a default database to be set upon the connection
};

/// Overloaded operator for serializing ConnectionParams instances
/// @note a value of the database password will be hashed in the output
std::ostream& operator<<(std::ostream&, ConnectionParams const&);

/**
 * Class DoNotProcess is an abstraction for SQL strings which than ordinary
 * values of string types needs to be injected into SQL statements without
 * being processed (escaped and quoted) as regular string values.
 */
class DoNotProcess {
public:
    /// @param name_ the input value
    explicit DoNotProcess(std::string const& name_);

    DoNotProcess() = delete;
    DoNotProcess(DoNotProcess const&) = default;
    DoNotProcess& operator=(DoNotProcess const&) = default;

    virtual ~DoNotProcess() = default;

    /// Unmodified value of an input string passed into the normal constructor of
    /// the class. The value would be inserted into queries by query generators.
    std::string name;
};

/**
 * Class Keyword is an abstraction for SQL keywords which needs to be processed
 * differently than ordinary values of string types. There won't be escape
 * processing or extra quotes of any kind added to the function name strings.
 */
class Keyword : public DoNotProcess {
public:
    /// @return the object representing the SQL keyword 'NULL'
    static Keyword const SQL_NULL;

    /// @param name_ the input value
    explicit Keyword(std::string const& name_);

    Keyword() = delete;
    Keyword(Keyword const&) = default;
    Keyword& operator=(Keyword const&) = default;
    ~Keyword() override = default;
};

/**
 * Class Function is an abstraction for SQL functions which needs to be processed
 * differently than ordinary values of string types. There won't be escape
 * processing or extra quotes of any kind added to the function name strings.
 */
class Function : public DoNotProcess {
public:
    /// @return the object representing the corresponding SQL function
    static Function const LAST_INSERT_ID;

    /// @param name_ the input value
    explicit Function(std::string const& name_);

    Function() = delete;
    Function(Function const&) = default;
    Function& operator=(Function const&) = default;

    ~Function() override = default;
};

/**
 * Objects of class Warning store rows extracted from a result set of MySQL
 * query "SHOW WARNINGS". Members of the class directly map to the corresponding
 * columns returned by the query. More info on this subject can be found in
 * the MySQL documentation for the query.
 */
class Warning {
public:
    explicit Warning(std::string const& level_, unsigned int code_, std::string const& message_)
            : level(level_), code(code_), message(message_) {}
    std::string level;
    unsigned int code = 0;
    std::string message;
};

}  // namespace lsst::qserv::replica::database::mysql

#endif  // LSST_QSERV_REPLICA_DATABASEMYSQLTYPES_H
