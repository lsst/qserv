// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST/AURA.
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
  * @file
  *
  * @brief Abstract Interface to the Common State System.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_CSS_KVINTERFACE_H
#define LSST_QSERV_CSS_KVINTERFACE_H

// System headers
#include <string>
#include <vector>

namespace lsst {
namespace qserv {
namespace css {

class KvInterface {
public:

    /**
     * Sets a max length for kvKey.
     * If it is changed it must also be changed in the
     * CssData SQL Schema as well.
     */
    static const unsigned int MAX_KEY_LENGTH = 255;

    virtual ~KvInterface() {};

    /**
     * Create a slash-delimited key-value pair.
     * Key must be shorter than MAX_KEY_LENGTH.
     * If the parent key does not exist it will be created with an empty value.
     * @throws KeyExistsError if the key already exists
     * @throws CssError for other problems (e.g., a connection error is detected).
     */
    virtual void create(std::string const& key, std::string const& value) = 0;

    /**
     * Set a key/value pair. If the key already exists, its value is
     * overwritten.
     * key must be shorter than MAX_KEY_LENGTH
     * @throws CssError when unable to set the pair (error with the underlying
     * persistence).
     */
    virtual void set(std::string const& key, std::string const& value) = 0;

    /**
     * Check if the key exists.
     */
    virtual bool exists(std::string const& key) = 0;

    /**
     * Returns value for a given key.
     * @throws NoSuchKey if the key is not found.
     * @throws CssError if there are any other problems, e.g., a connection
     * error is detected).
     */
    std::string get(std::string const& key) {
        return _get(key, std::string(), true);
    }

    /**
     * Returns value for a given key, defaultValue if the key does not exist.
     * @throws NoSuchKey if the key is not found.
     * @throws CssError if there are any other problems, e.g., a connection
     * error is detected).
     */
    std::string get(std::string const& key,
                    std::string const& defaultValue) {
        return _get(key, defaultValue, false);
    }

    /**
     * Returns children (vector of strings) for a given key.
     * @throws NoSuchKey if the key does not exist
     * @throws CssError for other problems (e.g., a connection error is detected).
     */
    virtual std::vector<std::string> getChildren(std::string const& key) = 0;

    /**
     * Delete a key, and all of its children (if they exist)
     * @throws NoSuchKey on failure.
     * @throws CssError for other problems.
     */
    virtual void deleteKey(std::string const& key) = 0;

protected:
    KvInterface() {}
    virtual std::string _get(std::string const& key,
                             std::string const& defaultValue,
                             bool throwIfKeyNotFound) = 0;
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_KVINTERFACE_H
