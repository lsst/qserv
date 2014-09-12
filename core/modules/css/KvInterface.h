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
    virtual ~KvInterface() {};

    /**
     * Create a key/value pair.
     * Throws CssRunTimeError if the key already exists (or if any other problem,
     * e.g., a connection error is detected).
     */
    virtual void create(std::string const& key, std::string const& value) = 0;

    /**
     * Check if the key exists.
     */
    virtual bool exists(std::string const& key) = 0;

    /**
     * Returns value for a given key.
     * Throws CssRunTimeError if there are any other problems, e.g., a connection
     * error is detected).
     * Throws CssNoSuchKey if the key is not found.
     */
    std::string get(std::string const& key) {
        return _get(key, std::string(), true);
    }

    /**
     * Returns value for a given key, defaultValue if the key does not exist.
     * Throws CssRunTimeError if there are any other problems, e.g., a connection
     * error is detected).
     */
    std::string get(std::string const& key,
                    std::string const& defaultValue) {
        return _get(key, defaultValue, false);
    }

    /**
     * Returns children (vector of strings) for a given key.
     * Throws CssRunTimeError if the key does not exist (or if any other problem,
     * e.g., a connection error is detected)
     */
    virtual std::vector<std::string> getChildren(std::string const& key) = 0;

    /**
     * Delete a key.
     * Throws CssRunTimeError on failure.
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
