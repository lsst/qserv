/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

/**
 * This header defines public classes used in the implementation of
 * the C++ wrapper of the MySQL C language library. This header is
 * not supposed to be included directly by user's code.
 *
 * @see class Connection
 */

// System headers
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {

/**
 * Structure ConnectionParams encapsulates connection parameters to
 * a MySQL server. If constructed using the default constructor
 * the parameters will be initialized with some reasonable defaults:
 *
 *   host: localhost
 *   port: 3306
 *   user: effective user of a process
 *
 * The following parameters will be empty:
 *
 *   password:
 *   database:
 */
struct ConnectionParams {

    /// The DNS name or IP address of a machine where the database
    /// server runs
    std::string host;

    /// The port number of the MySQL service
    uint16_t port;

    /// The name of a database user
    std::string user;

    /// The database password
    std::string password;

    /// The name of a database to be set upon the connection
    std::string database;

    /**
     * The factory method will return an instance of this structure initialized
     * by values of parameters found in the input encoded string. The string is
     * expected to have the following syntax:
     *
     *   mysql://[user][:password]@[host][:port][/database]
     *
     * NOTES ON THE SYNTAX:
     * 1) all keywords are mandatory
     * 2) the corresponding values for for all but the database are optional
     * 3) default values for other parameters (if missing in the string) will be assumed.
     *
     * @param params          - connection parameters packed into a string
     * @param defaultHost     - default value for a host name
     * @param defaultPort     - default port number
     * @param defaultUser     - default value for a database user account
     * @param defaultPassword - default value for a database user account
     *
     * @throw std::invalid_argument - if the string can't be parsed
     */
    static ConnectionParams parse(std::string const& params,
                                  std::string const& defaultHost,
                                  uint16_t defaultPort,
                                  std::string const& defaultUser,
                                  std::string const& defaultPassword);

    /// Default constructor will initialize connection parameters
    /// with default values
    ConnectionParams();

    /// Normal constructor
    ConnectionParams(std::string const& host_,
                     uint16_t port_,
                     std::string const& user_,
                     std::string const& password_,
                     std::string const& database_);

    /**
     * Return a string representation of all (but the password) parameters.
     * The result will be formatted similarly to the one expected by
     * the non-default constructor of the class.
     */
    std::string toString() const;
};

/// Overloaded operator for serializing ConnectionParams instances
std::ostream& operator<<(std::ostream&, ConnectionParams const&);

/**
 * Class DoNotProcess is an abstraction for SQL strings which than ordinary
 * values of string types needs to be injected into SQL statements without
 * being processed (escaped and quoted) as regular string values.
 */
class DoNotProcess {

public:

    /**
     * The normal constructor
     *
     * @param name_ - the input value
     */
    explicit DoNotProcess(std::string const& name_);

    DoNotProcess() = delete;

    DoNotProcess(DoNotProcess const&) = default;
    DoNotProcess& operator=(DoNotProcess const&) = default;

    virtual ~DoNotProcess() = default;

public:

    /**
     * The exact string value as it should appear within queries. It will
     * be extracted by the corresponding query generators.
     */
    std::string name;
};

/**
 * Class Keyword is an abstraction for SQL keywords which needs to be processed
 * differently than ordinary values of string types. There won't be escape
 * processing or extra quotes of any kind added to the function name strings.
 */
class Keyword
    :   public DoNotProcess {

public:

    // Predefined SQL keywords

    /// @return the object representing the corresponding SQL keyword
    static Keyword const SQL_NULL;

    /**
     * The normal constructor
     *
     * @param name_ - the input value
     */
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
class Function
    :   public DoNotProcess {

public:

    /// @return the object representing the corresponding SQL function
    static Function const LAST_INSERT_ID;

    /**
     * The normal constructor
     *
     * @param name_ - the input value
     */
    explicit Function(std::string const& name_);

    Function() = delete;

    Function(Function const&) = default;
    Function& operator=(Function const&) = default;

    ~Function() override = default;
};

}}}}} // namespace lsst::qserv::replica::database::mysql

#endif // LSST_QSERV_REPLICA_DATABASEMYSQLTYPES_H
