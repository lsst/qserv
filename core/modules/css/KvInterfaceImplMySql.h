// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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

/**
  * @file KvInterfaceImplMySql.h
  *
  * @brief Interface to the Central State System - in MySql storage
  *
  * @Author Nathan Pease, SLAC
  */


#ifndef LSST_QSERV_CSS_KVINTERFACEIMPLMYSQL_H
#define LSST_QSERV_CSS_KVINTERFACEIMPLMYSQL_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Local headers
#include "css/KvInterface.h"
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnection.h"

namespace lsst {
namespace qserv {
namespace css {

class KvTransaction;

class KvInterfaceImplMySql : public KvInterface {
public:

    /**
     *  @param mysqlConf: Configuration object for mysql connection
     */
    KvInterfaceImplMySql(mysql::MySqlConfig const& mysqlConf);
    virtual ~KvInterfaceImplMySql() {};

    virtual std::string create(std::string const& key, std::string const& value,
                               bool unique=false) override;

    virtual void set(std::string const& key, std::string const& value) override;

    virtual bool exists(std::string const& key) override;

    virtual std::map<std::string, std::string> getMany(std::vector<std::string> const& keys) override;

    virtual std::vector<std::string> getChildren(std::string const& parentKey) override;

    virtual void deleteKey(std::string const& key) override;

protected:
    virtual std::string _get(std::string const& key,
                             std::string const& defaultValue,
                             bool throwIfKeyNotFound) override;
private:
    /**
     * @brief Returns children with full path (vector of strings) for a given key.
     */
    std::vector<std::string> _getChildrenFullPath(std::string const& parentKey, KvTransaction const& transaction);

    /**
     * @brief Get the id from the server for a given key.
     *
     * Returns true if key was found, false if key was not found on server.
     */
    bool _getIdFromServer(std::string const& key, unsigned int* id, KvTransaction const& transaction);

    /**
     * @brief Get the parent id of a given child kvKey.
     *
     * @note This gets used by the create member function. If the parent of the named child key does
     * not exist then the parent will be created (recursively).
     */
    void _findParentId(std::string const& childKvKey, bool* hasParent, unsigned int* parentKvId, KvTransaction const& transaction);

    /**
     * @brief create a key value pair in the KV database
     * @param key
     * @param value
     * @param updateIfExists flag to update if the key already exists (used by set)
     * @return the kvId of the key value pair entry in the database
     */
    unsigned int _create(std::string const& key, std::string const& value, bool updateIfExists, KvTransaction const& transaction);

    /**
     * @brief delete the entry for key, and all of its children if applicable
     */
    void _delete(std::string const& key, KvTransaction const& transaction);

    /**
     * @brief Validate key string our key rules.
     * @param key
     * Verifies that a key is formatted correctly. It must:
     * - start with a slash (/)
     * - not end with a slash
     * - must not exceed the maximum length for a key
     * @note Will connect to the local connection if needed.
     * @throws CssError if key fails validation.
     */
    void _validateKey(std::string const& key);

    /**
     * @brief Escape a string for sql.
     * @param value will be escaped as needed
     * Will connect to the local connection if needed.
     * @throws CssErrror if connection fails
     * @return the escaped string
     */
    std::string _escapeSqlString(std::string const& str);

    sql::SqlConnection _conn;
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_KVINTERFACEIMPLMYSQL_H
